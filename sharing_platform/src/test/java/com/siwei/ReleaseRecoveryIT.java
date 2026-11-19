package com.siwei;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.siwei.endpoint.*;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

class ReleaseRecoveryIT {
    private static final String URL = TestDatabase.requireUrl(System.getenv());
    private static final String USER = TestDatabase.require(System.getenv(), "TEST_DB_USERNAME");
    private static final String PASSWORD = TestDatabase.require(System.getenv(), "TEST_DB_PASSWORD");
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private static final Pattern PORT = Pattern.compile("Tomcat started on port\\(s\\): ([0-9]+)");
    private static final String TOKEN = "recovery-fixture-only";
    @TempDir Path root;

    @ParameterizedTest @EnumSource(EndpointRevision.Protocol.class)
    void killedApplicationRecoversTheSamePublishedPullThenAcceptance(EndpointRevision.Protocol protocol) throws Exception {
        String schema = "recovery_it_" + UUID.randomUUID().toString().replace("-", "");
        String endpoint = UUID.randomUUID().toString(), revision = UUID.randomUUID().toString();
        String appName = schema + "_child";
        Path remoteRoot = Files.createDirectory(root.resolve("remote"));
        Path local = Files.createDirectory(root.resolve("delivery"));
        var key = new EndpointSettings();
        byte[] random = new byte[32];
        new java.security.SecureRandom().nextBytes(random);
        key.setMasterKey(Base64.getEncoder().encodeToString(random));
        HttpServer receiver = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var notices = new java.util.concurrent.CopyOnWriteArrayList<String>();
        receiver.createContext("/notify", exchange -> {
            String body = new String(exchange.getRequestBody().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            notices.add(body);
            var notice = JSON.readTree(body);
            byte[] ack = JSON.writeValueAsBytes(Map.of("accepted", true, "notificationId", notice.path("notificationId").asText(),
                    "taskRecordId", notice.path("taskRecordId").asText()));
            exchange.sendResponseHeaders(200, ack.length);
            try (var output = exchange.getResponseBody()) { output.write(ack); }
        });
        receiver.start();
        try (ProtocolServers server = new ProtocolServers(remoteRoot, protocol, ProtocolServers.Mode.NORMAL);
             Connection database = DriverManager.getConnection(URL, USER, PASSWORD)) {
            Flyway.configure().dataSource(URL, USER, PASSWORD).defaultSchema(schema).load().migrate();
            database.setSchema(schema);
            try (Statement sql = database.createStatement()) {
                sql.execute("INSERT INTO endpoint(id,revision,enabled) VALUES ('" + endpoint + "',1,true)");
                sql.execute("CREATE FUNCTION hold_completion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(45); RETURN NEW; END $$");
                sql.execute("CREATE TRIGGER hold_completion BEFORE INSERT ON transfer_notification FOR EACH ROW EXECUTE PROCEDURE hold_completion()");
            }
            try (PreparedStatement insert = database.prepareStatement("INSERT INTO endpoint_revision(id,endpoint_id,revision,name,protocol,host,port,"
                    + "username,auth,secret,fingerprint,read_root,local_target_ref,timeout_seconds,concurrency,passive,isolated_ftp_root,created_at)"
                    + " VALUES (?,?,1,'Recovery test',?,'127.0.0.1',?,'tester','PASSWORD',?,?,'/','delivery',5,1,true,true,now())")) {
                insert.setString(1, revision); insert.setString(2, endpoint); insert.setString(3, protocol.name());
                insert.setInt(4, server.port); insert.setString(5, new CredentialCipher(key).encrypt(ProtocolServers.PASSWORD, revision));
                insert.setString(6, protocol == EndpointRevision.Protocol.SFTP ? server.fingerprint : null);
                insert.executeUpdate();
            }
            byte[] expected = Files.readAllBytes(remoteRoot.resolve("sample.bin"));
            String request = JSON.writeValueAsString(Map.of("taskId", "restart-task", "direction", "PULL", "endpointId", endpoint,
                    "sourceId", "recovery", "sourcePath", "sample.bin", "targetPath", "delivery"));
            String task, transfer, oldClaim;
            try (Running acceptedApp = start(schema, appName, endpoint, local, key, receiver.getAddress().getPort(), "accepted")) {
                HttpResponse<String> accepted = post(acceptedApp, "/api/transfers/tasks", request);
                assertThat(accepted.statusCode()).isEqualTo(202);
                task = JSON.readTree(accepted.body()).path("taskRecordId").asText();
                assertThat(scalar(database, "SELECT status FROM transfer_task WHERE id=?", task)).isEqualTo("QUEUED");
                try (Statement sql = database.createStatement(); ResultSet lock = sql.executeQuery(
                        "SELECT pg_try_advisory_lock(hashtext(current_schema()), 150080831)")) {
                    assertThat(lock.next()).isTrue();
                    assertThat(lock.getBoolean(1)).isFalse();
                }
                acceptedApp.kill();
            }
            disconnectChild(database, appName);
            try (Running first = start(schema, appName, endpoint, local, key, receiver.getAddress().getPort(), "publishing")) {
                StreamingFtpServer.await(() -> {
                    String id = scalar(database, "SELECT id FROM pull_transfer WHERE owner_task_id=?", task);
                    return id != null && Files.isRegularFile(local.resolve("pending").resolve(id).resolve(".sharing-complete"));
                }, Duration.ofSeconds(30));
                transfer = scalar(database, "SELECT id FROM pull_transfer WHERE owner_task_id=?", task);
                oldClaim = scalar(database, "SELECT claim_id FROM transfer_task WHERE id=?", task);
                assertThat(oldClaim).isNotBlank();
                assertThat(scalar(database, "SELECT status FROM pull_transfer WHERE id=?", transfer)).isEqualTo("PUBLISHING");
                first.kill();
            }
            disconnectChild(database, appName);
            try (Statement sql = database.createStatement()) { sql.execute("DROP TRIGGER hold_completion ON transfer_notification"); }
            Files.delete(remoteRoot.resolve("sample.bin"));
            try (Running restarted = start(schema, appName, endpoint, local, key, receiver.getAddress().getPort(), "after")) {
                StreamingFtpServer.await(() -> "WAITING_ACCEPTANCE".equals(scalar(database, "SELECT status FROM transfer_task WHERE id=?", task))
                        && "ACKED".equals(scalar(database, "SELECT status FROM transfer_notification WHERE transfer_id=?", transfer)), Duration.ofSeconds(30));
                assertThat(Files.readAllBytes(local.resolve("pending").resolve(transfer).resolve("sample.bin"))).isEqualTo(expected);
                assertThat(scalar(database, "SELECT cast(count(*) AS varchar) FROM pull_transfer WHERE owner_task_id=?", task)).isEqualTo("1");
                assertThat(scalar(database, "SELECT cast(count(*) AS varchar) FROM transfer_notification WHERE transfer_id=?", transfer)).isEqualTo("1");
                assertThat(notices).hasSize(1);
                assertThat(JSON.readTree(post(restarted, "/api/transfers/tasks", request).body()).path("taskRecordId").asText()).isEqualTo(task);
                String receipt = JSON.writeValueAsString(Map.of("transferId", transfer, "result", "ACCEPTED", "sourceVersion",
                        scalar(database, "SELECT source_version FROM pull_transfer WHERE id=?", transfer)));
                assertThat(post(restarted, "/api/sharing/pull/confirmations", receipt).statusCode()).isEqualTo(200);
                assertThat(post(restarted, "/api/sharing/pull/confirmations", receipt).statusCode()).isEqualTo(200);
                assertThat(scalar(database, "SELECT status FROM transfer_task WHERE id=?", task)).isEqualTo("SUCCEEDED");
                System.out.printf("RELEASE_RECOVERY protocol=%s forced_stops=2 deliveries=1 notifications=1%n", protocol);
            }
        } finally {
            receiver.stop(0);
            if (!schema.matches("recovery_it_[a-f0-9]{32}")) throw new IllegalStateException("Unsafe test schema");
            try (Connection database = DriverManager.getConnection(URL, USER, PASSWORD); Statement sql = database.createStatement()) {
                sql.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
            }
        }
    }

    private static void disconnectChild(Connection database, String appName) throws SQLException {
        try (PreparedStatement terminate = database.prepareStatement("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                + "WHERE datname=current_database() AND application_name=? AND pid<>pg_backend_pid()")) {
            terminate.setString(1, appName); terminate.execute();
        }
    }

    private Running start(String schema, String name, String endpoint, Path local, EndpointSettings key, int receiver, String run) throws Exception {
        Path jar;
        try (var paths = Files.list(Path.of("target"))) {
            jar = paths.filter(path -> path.getFileName().toString().matches("sharing_platform-.*[.]jar")).findFirst().orElseThrow();
        }
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Path log = root.resolve(run + ".log");
        ProcessBuilder builder = new ProcessBuilder(java, "-jar", jar.toString(), "--spring.profiles.active=test", "--server.port=0",
                "--spring.datasource.hikari.schema=" + schema, "--spring.flyway.default-schema=" + schema);
        String config = JSON.writeValueAsString(Map.of("spring", Map.of("datasource", Map.of("hikari", Map.of("connection-init-sql",
                "SET application_name TO '" + name + "'"))), "platform", Map.of(
                "recovery", Map.of("test-instance-lock", true, "dispatch-enabled", !run.equals("accepted")),
                "endpoints", Map.of("master-key", key.getMasterKey(), "local-roots", Map.of("delivery", local.toString())),
                "transfers", Map.of("callers", Map.of("test", Map.of("token", TOKEN, "endpoint-ids", List.of(endpoint),
                        "notification-url", "http://127.0.0.1:" + receiver + "/notify"))),
                "pulls", Map.of("sources", Map.of("recovery", Map.of("endpoint-id", endpoint, "source-path", "sample.bin", "target-ref", "delivery",
                        "recipient-id", "test", "caller-ids", List.of("test"), "access-base", "https://business.example/data", "stable-observations", 1))))));
        builder.environment().put("SPRING_APPLICATION_JSON", config);
        Process child = builder.redirectErrorStream(true).redirectOutput(log.toFile()).start();
        var running = new Running(child);
        try {
            StreamingFtpServer.await(() -> {
                if (!child.isAlive()) throw new IllegalStateException("Recovery JVM exited before listening: " + safeRead(log));
                var matcher = PORT.matcher(safeRead(log));
                if (!matcher.find()) return false;
                running.port = Integer.parseInt(matcher.group(1));
                return true;
            }, Duration.ofSeconds(45));
            return running;
        } catch (Exception | AssertionError error) { running.close(); throw error; }
    }

    private static String safeRead(Path path) {
        try { return Files.readString(path, java.nio.charset.Charset.defaultCharset()); }
        catch (IOException error) { return ""; }
    }

    private static String scalar(Connection connection, String sql, String key) {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, key);
            try (ResultSet result = statement.executeQuery()) { return result.next() ? result.getString(1) : null; }
        } catch (SQLException error) { throw new IllegalStateException(error); }
    }

    private static HttpResponse<String> post(Running app, String path, String body) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + app.port + path)).timeout(Duration.ofSeconds(10))
                .header("Authorization", "Bearer " + TOKEN).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private static final class Running implements AutoCloseable {
        final Process child;
        int port;
        Running(Process child) { this.child = child; }
        void kill() throws InterruptedException {
            if (child.isAlive()) child.destroyForcibly();
            assertThat(child.waitFor(15, TimeUnit.SECONDS)).isTrue();
        }
        @Override public void close() throws InterruptedException { kill(); }
    }
}
