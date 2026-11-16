package com.siwei;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.siwei.endpoint.*;
import com.siwei.protocol.RemoteFileClient;
import com.siwei.protocol.RemoteFileClientFactory;
import com.siwei.transfer.*;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.DriverManager;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

@SpringBootTest
@ActiveProfiles("test")
@DirtiesContext
class PushIT {
    private static final String URL = TestDatabase.requireUrl(System.getenv());
    private static final String USER = TestDatabase.require(System.getenv(), "TEST_DB_USERNAME");
    private static final String PASSWORD = TestDatabase.require(System.getenv(), "TEST_DB_PASSWORD");
    private static final String SCHEMA = "push_it_" + UUID.randomUUID().toString().replace("-", "");
    private static final Path SOURCE;
    private static final AtomicBoolean refusePrecheckAck = new AtomicBoolean();
    private static final AtomicBoolean refuseCompleteAck = new AtomicBoolean();
    private static HttpServer receiver;
    static {
        try { SOURCE = Files.createTempDirectory("push-it-source-"); }
        catch (java.io.IOException error) { throw new ExceptionInInitializerError(error); }
    }

    private static synchronized int receiverPort() {
        if (receiver == null) try {
            receiver = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            receiver.createContext("/notify", exchange -> {
                var node = new ObjectMapper().readTree(exchange.getRequestBody());
                String stage = node.path("stage").asText();
                String id = (refusePrecheckAck.get() && stage.equals("PRECHECK")
                        || refuseCompleteAck.get() && stage.equals("COMPLETE"))
                        ? "wrong" : node.path("notificationId").asText();
                byte[] ack = ("{\"accepted\":true,\"notificationId\":\"" + id + "\",\"taskRecordId\":\""
                        + node.path("taskRecordId").asText() + "\"}").getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, ack.length);
                try (var out = exchange.getResponseBody()) { out.write(ack); }
            });
            receiver.start();
        } catch (java.io.IOException error) { throw new IllegalStateException(error); }
        return receiver.getAddress().getPort();
    }

    @DynamicPropertySource static void properties(DynamicPropertyRegistry p) {
        p.add("spring.datasource.url", () -> URL); p.add("spring.datasource.username", () -> USER);
        p.add("spring.datasource.password", () -> PASSWORD); p.add("spring.datasource.hikari.schema", () -> SCHEMA);
        p.add("spring.flyway.default-schema", () -> SCHEMA);
        p.add("platform.endpoints.master-key", () -> Base64.getEncoder().encodeToString(new byte[32]));
        p.add("platform.endpoints.local-roots.source", () -> SOURCE.toString());
        p.add("platform.transfers.callers.push.token", () -> "push-test-token");
        p.add("platform.transfers.callers.push.notification-url", () -> "http://127.0.0.1:" + receiverPort() + "/notify");
    }

    @Autowired EndpointService endpoints;
    @Autowired EndpointRevisionRepository revisions;
    @Autowired PushProcessor push;
    @Autowired JdbcTemplate db;
    @Autowired TaskService tasks;
    @Autowired TaskWorker worker;
    @Autowired NotificationDispatcher notifications;
    @Autowired TransferSettings transferSettings;
    @Autowired RemoteFileClientFactory clients;
    @Autowired ObjectMapper json;
    @Autowired PlatformTransactionManager transactionManager;
    @TempDir Path remoteRoot;

    @ParameterizedTest @EnumSource(EndpointRevision.Protocol.class)
    void pushesNestedProductsWithManifestAndReady(EndpointRevision.Protocol protocol) throws Exception {
        Path first = SOURCE.resolve("one"), second = SOURCE.resolve("two");
        Files.createDirectories(first.resolve("nested")); Files.createDirectories(first.resolve("empty"));
        Files.createDirectories(second);
        Files.writeString(first.resolve("nested/a.bin"), "first payload");
        Files.writeString(second.resolve("b.bin"), "second payload");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, protocol, ProtocolServers.Mode.NORMAL)) {
            EndpointForm form = server.form(protocol); form.localSourceRef = "source";
            String endpointId = endpoints.save(null, form);
            String revisionId = revisions.findByEndpointIdAndRevision(endpointId, 1).orElseThrow().id;
            Task task = task(endpointId, revisionId);
            TaskProcessor.Result precheck = push.process(task);
            assertThat(precheck.status).describedAs("precheck: %s / %s", precheck.errorType, precheck.errorDetail)
                    .isEqualTo("WAITING_FEEDBACK");
            assertThat(Files.exists(remoteRoot.resolve("deliver/weather/" + task.taskRecordId + "/_READY"))).isFalse();
            task.phase = "PRECHECK_ACKED";
            assertThat(push.process(task).status).isEqualTo("SUCCEEDED");
            Path delivered = remoteRoot.resolve("deliver/weather/" + task.taskRecordId);
            assertThat(Files.readString(delivered.resolve("p1/nested/a.bin"))).isEqualTo("first payload");
            assertThat(Files.readString(delivered.resolve("p2/b.bin"))).isEqualTo("second payload");
            assertThat(Files.isDirectory(delivered.resolve("p1/empty"))).isTrue();
            assertThat(Files.readString(delivered.resolve("_READY"))).contains("taskId=" + task.taskRecordId, "productCount=2");
            assertThat(Files.readString(delivered.resolve("_manifest.xml"))).contains("<directory path=\"p1/empty\"/>", "sha256=");
            assertThat(db.queryForObject("SELECT count(*) FROM transfer_file WHERE task_id=? AND verified", Integer.class, task.taskRecordId)).isEqualTo(2);
            assertThat(push.process(task).status).isEqualTo("SUCCEEDED");
        }
    }

    @Test void sameSizeSourceChangeAfterPrecheckNeverPublishes() throws Exception {
        Path product = SOURCE.resolve("changed"); Files.createDirectories(product);
        Files.writeString(product.resolve("a.bin"), "original");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            EndpointForm form = server.form(EndpointRevision.Protocol.FTP); form.localSourceRef = "source";
            String endpointId = endpoints.save(null, form);
            Task task = task(endpointId, revisions.findByEndpointIdAndRevision(endpointId, 1).orElseThrow().id);
            task.products = List.of(Map.of("product_id", "p1", "product_path", "changed"));
            assertThat(push.process(task).status).isEqualTo("WAITING_FEEDBACK");
            Files.writeString(product.resolve("a.bin"), "replaced");
            task.phase = "PRECHECK_ACKED";
            assertThatThrownBy(() -> push.process(task)).isInstanceOf(PushProcessor.SourceChanged.class);
            assertThat(Files.exists(remoteRoot.resolve("deliver/weather/" + task.taskRecordId + "/_READY"))).isFalse();
        }
    }

    @Test void sameSizeCorruptTargetNeverPublishes() throws Exception {
        Path product = SOURCE.resolve("corrupt"); Files.createDirectories(product);
        Files.writeString(product.resolve("a.bin"), "original");
        Path other = SOURCE.resolve("corrupt-other"); Files.createDirectories(other);
        Files.writeString(other.resolve("b.bin"), "original");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            EndpointForm form = server.form(EndpointRevision.Protocol.FTP); form.localSourceRef = "source";
            String endpointId = endpoints.save(null, form);
            Task task = task(endpointId, revisions.findByEndpointIdAndRevision(endpointId, 1).orElseThrow().id);
            task.products = List.of(Map.of("product_id", "p1", "product_path", "corrupt"),
                    Map.of("product_id", "p2", "product_path", "corrupt-other"));
            assertThat(push.process(task).status).isEqualTo("WAITING_FEEDBACK");
            Path delivered = remoteRoot.resolve("deliver/weather/" + task.taskRecordId);
            Files.createDirectories(delivered.resolve("p2"));
            Files.writeString(delivered.resolve("p2/b.bin"), "replaced");
            task.phase = "PRECHECK_ACKED";
            assertThatThrownBy(() -> push.process(task)).isInstanceOf(java.io.IOException.class).hasMessage("TARGET_HASH_MISMATCH");
            assertThat(Files.exists(delivered.resolve("_READY"))).isFalse();
        }
    }

    @Test void extraTargetFileBlocksWholePublication() throws Exception {
        Path product = SOURCE.resolve("extra"); Files.createDirectories(product);
        Files.writeString(product.resolve("a.bin"), "payload");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            EndpointForm form = server.form(EndpointRevision.Protocol.FTP); form.localSourceRef = "source";
            String endpointId = endpoints.save(null, form);
            Task task = task(endpointId, revisions.findByEndpointIdAndRevision(endpointId, 1).orElseThrow().id);
            task.products = List.of(Map.of("product_id", "p1", "product_path", "extra"));
            assertThat(push.process(task).status).isEqualTo("WAITING_FEEDBACK");
            Path delivered = remoteRoot.resolve("deliver/weather/" + task.taskRecordId);
            Files.createDirectories(delivered);
            Files.writeString(delivered.resolve("intruder.bin"), "unexpected");
            task.phase = "PRECHECK_ACKED";
            assertThatThrownBy(() -> push.process(task)).isInstanceOf(java.io.IOException.class).hasMessage("EXTRA_TARGET_FILE");
            assertThat(Files.exists(delivered.resolve("_READY"))).isFalse();
        }
    }

    @Test void targetWithoutReadbackIsRejectedBeforeBusinessFiles() throws Exception {
        Path product = SOURCE.resolve("writeonly"); Files.createDirectories(product);
        Files.writeString(product.resolve("a.bin"), "payload");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.SFTP, ProtocolServers.Mode.WRITE_ONLY)) {
            EndpointForm form = server.form(EndpointRevision.Protocol.SFTP); form.localSourceRef = "source";
            String endpointId = endpoints.save(null, form);
            Task task = task(endpointId, revisions.findByEndpointIdAndRevision(endpointId, 1).orElseThrow().id);
            task.products = List.of(Map.of("product_id", "p1", "product_path", "writeonly"));
            assertThat(push.process(task).status).isEqualTo("REJECTED");
            assertThat(Files.exists(remoteRoot.resolve("deliver/weather/" + task.taskRecordId))).isFalse();
        }
    }

    @Test void acceptedTaskKeepsEndpointRevisionAfterDisable() throws Exception {
        Path product = SOURCE.resolve("snapshot"); Files.createDirectories(product);
        Files.writeString(product.resolve("a.bin"), "snapshot payload");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            EndpointForm form = server.form(EndpointRevision.Protocol.FTP); form.localSourceRef = "source";
            String endpointId = endpoints.save(null, form);
            Task task = task(endpointId, revisions.findByEndpointIdAndRevision(endpointId, 1).orElseThrow().id);
            task.products = List.of(Map.of("product_id", "p1", "product_path", "snapshot"));
            assertThat(push.process(task).status).isEqualTo("WAITING_FEEDBACK");
            endpoints.toggle(endpointId, 1);
            task.phase = "PRECHECK_ACKED";
            assertThat(push.process(task).status).isEqualTo("SUCCEEDED");
            transferSettings.getCallers().get("push").getEndpointIds().add(endpointId);
            String request = "{\"taskId\":\"new-after-disable\",\"direction\":\"PUSH\",\"endpointId\":\"" + endpointId
                    + "\",\"sourceRoot\":\"source\",\"targetPath\":\"deliver\","
                    + "\"products\":[{\"productId\":\"p1\",\"productPath\":\"snapshot\"}]}";
            assertThatThrownBy(() -> tasks.accept("push", tasks.parseJson(request), request, "JSON"))
                    .isInstanceOf(org.springframework.web.server.ResponseStatusException.class);
        }
    }

    @Test void readyRenameSucceededButReplyLostReconcilesWithoutReupload() throws Exception {
        Path product = SOURCE.resolve("lost-reply"); Files.createDirectories(product);
        Files.writeString(product.resolve("a.bin"), "fixed content");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            EndpointForm form = server.form(EndpointRevision.Protocol.FTP); form.localSourceRef = "source";
            String endpointId = endpoints.save(null, form);
            Task task = task(endpointId, revisions.findByEndpointIdAndRevision(endpointId, 1).orElseThrow().id);
            task.products = List.of(Map.of("product_id", "p1", "product_path", "lost-reply"));
            assertThat(push.process(task).status).isEqualTo("WAITING_FEEDBACK");
            task.phase = "PRECHECK_ACKED";
            RemoteFileClientFactory faulty = mock(RemoteFileClientFactory.class);
            when(faulty.connect(any(EndpointRevision.class))).thenAnswer(invocation -> {
                RemoteFileClient real = clients.connect(invocation.getArgument(0));
                return java.lang.reflect.Proxy.newProxyInstance(RemoteFileClient.class.getClassLoader(),
                        new Class<?>[] {RemoteFileClient.class}, (proxy, method, arguments) -> {
                            try {
                                Object value = method.invoke(real, arguments);
                                if (method.getName().equals("rename") && ((String) arguments[1]).endsWith("/_READY")) {
                                    real.close();
                                    throw new java.io.IOException("LOST_READY_REPLY");
                                }
                                return value;
                            } catch (java.lang.reflect.InvocationTargetException error) { throw error.getCause(); }
                        });
            });
            PushProcessor interrupted = new PushProcessor(db, json, revisions, transferSettings, faulty, transactionManager);
            assertThatThrownBy(() -> interrupted.process(task)).isInstanceOf(java.io.IOException.class);
            Path delivered = remoteRoot.resolve("deliver/weather/" + task.taskRecordId);
            assertThat(Files.readString(delivered.resolve("_READY"))).contains("taskId=" + task.taskRecordId);
            var modified = Files.getLastModifiedTime(delivered.resolve("p1/a.bin"));
            assertThat(push.process(task).status).isEqualTo("SUCCEEDED");
            assertThat(Files.getLastModifiedTime(delivered.resolve("p1/a.bin"))).isEqualTo(modified);
        }
    }

    @Test void interruptedFileStreamRestartsSameFileAndPublishesOnce() throws Exception {
        Path product = SOURCE.resolve("interrupted"); Files.createDirectories(product);
        Files.writeString(product.resolve("a.bin"), "long enough to break midstream");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            EndpointForm form = server.form(EndpointRevision.Protocol.FTP); form.localSourceRef = "source";
            String endpointId = endpoints.save(null, form);
            Task task = task(endpointId, revisions.findByEndpointIdAndRevision(endpointId, 1).orElseThrow().id);
            task.products = List.of(Map.of("product_id", "p1", "product_path", "interrupted"));
            assertThat(push.process(task).status).isEqualTo("WAITING_FEEDBACK");
            task.phase = "PRECHECK_ACKED";
            RemoteFileClientFactory faulty = mock(RemoteFileClientFactory.class);
            when(faulty.connect(any(EndpointRevision.class))).thenAnswer(invocation -> {
                RemoteFileClient real = clients.connect(invocation.getArgument(0));
                return java.lang.reflect.Proxy.newProxyInstance(RemoteFileClient.class.getClassLoader(),
                        new Class<?>[] {RemoteFileClient.class}, (proxy, method, arguments) -> {
                            try {
                                if (method.getName().equals("write") && ((String) arguments[0]).contains("/.sharing-file-")) {
                                    java.io.InputStream broken = new java.io.FilterInputStream((java.io.InputStream) arguments[1]) {
                                        int remaining = 4;
                                        @Override public int read(byte[] bytes, int offset, int length) throws java.io.IOException {
                                            if (remaining == 0) throw new java.io.IOException("NETWORK_LOST");
                                            int count = in.read(bytes, offset, Math.min(length, remaining));
                                            if (count > 0) remaining -= count;
                                            return count;
                                        }
                                    };
                                    return method.invoke(real, arguments[0], broken);
                                }
                                return method.invoke(real, arguments);
                            } catch (java.lang.reflect.InvocationTargetException error) { throw error.getCause(); }
                        });
            });
            PushProcessor interrupted = new PushProcessor(db, json, revisions, transferSettings, faulty, transactionManager);
            assertThatThrownBy(() -> interrupted.process(task)).isInstanceOf(java.io.IOException.class);
            assertThat(push.process(task).status).isEqualTo("SUCCEEDED");
            Path delivered = remoteRoot.resolve("deliver/weather/" + task.taskRecordId);
            assertThat(Files.readString(delivered.resolve("p1/a.bin"))).isEqualTo("long enough to break midstream");
            assertThat(Files.exists(delivered.resolve("_READY"))).isTrue();
        }
    }

    @Test void precheckAckGatesRealPushAndCompletionIsAcknowledged() throws Exception {
        Path product = SOURCE.resolve("workflow"); Files.createDirectories(product);
        Files.writeString(product.resolve("a.bin"), "workflow payload");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            EndpointForm form = server.form(EndpointRevision.Protocol.FTP); form.localSourceRef = "source";
            String endpointId = endpoints.save(null, form);
            transferSettings.getCallers().get("push").getEndpointIds().add(endpointId);
            String request = "{\"taskId\":\"workflow\",\"direction\":\"PUSH\",\"endpointId\":\"" + endpointId
                    + "\",\"sourceCode\":\"weather\",\"sourceRoot\":\"source\",\"targetPath\":\"deliver\",\"fileCount\":1,"
                    + "\"products\":[{\"productId\":\"p1\",\"productPath\":\"workflow\"}]}";
            refusePrecheckAck.set(true);
            refuseCompleteAck.set(true);
            var accepted = tasks.accept("push", tasks.parseJson(request), request, "JSON");
            await(() -> "RETRY_WAIT".equals(db.queryForObject("SELECT status FROM transfer_notification WHERE task_id=? AND stage='PRECHECK'", String.class, accepted.taskRecordId)));
            assertThat(Files.exists(remoteRoot.resolve("deliver/weather/" + accepted.taskRecordId))).isFalse();
            refusePrecheckAck.set(false);
            db.update("UPDATE transfer_notification SET next_attempt_at=now() WHERE task_id=? AND stage='PRECHECK'", accepted.taskRecordId);
            await(() -> {
                notifications.poll(); worker.poll();
                return "ACKED".equals(db.queryForObject("SELECT status FROM transfer_notification WHERE task_id=? AND stage='PRECHECK'", String.class, accepted.taskRecordId));
            });
            await(() -> {
                worker.poll(); notifications.poll();
                return "RETRY_WAIT".equals(db.queryForObject("SELECT status FROM transfer_notification WHERE task_id=? AND stage='COMPLETE'", String.class, accepted.taskRecordId));
            });
            Path deliveredFile = remoteRoot.resolve("deliver/weather/" + accepted.taskRecordId + "/p1/a.bin");
            var modified = Files.getLastModifiedTime(deliveredFile);
            refuseCompleteAck.set(false);
            db.update("UPDATE transfer_notification SET next_attempt_at=now() WHERE task_id=? AND stage='COMPLETE'", accepted.taskRecordId);
            await(() -> {
                notifications.poll();
                return "ACKED".equals(db.queryForObject("SELECT status FROM transfer_notification WHERE task_id=? AND stage='COMPLETE'", String.class, accepted.taskRecordId));
            });
            assertThat(Files.getLastModifiedTime(deliveredFile)).isEqualTo(modified);
            assertThat(tasks.get("push", accepted.taskRecordId).executionStatus).isEqualTo("SUCCEEDED");
            assertThat(tasks.get("push", accepted.taskRecordId).verifiedBytes).isEqualTo((long) "workflow payload".length());
            assertThat(Files.readString(remoteRoot.resolve("deliver/weather/" + accepted.taskRecordId + "/p1/a.bin"))).isEqualTo("workflow payload");
            assertThat(db.queryForObject("SELECT payload FROM transfer_notification WHERE task_id=? AND stage='COMPLETE'", String.class, accepted.taskRecordId))
                    .contains("deliver/weather/" + accepted.taskRecordId + "/p1", "\"fileCount\":1");
            assertThat(tasks.files("push", accepted.taskRecordId, 0, 50).get("manifestFrozen")).isEqualTo(true);
        } finally { refusePrecheckAck.set(false); refuseCompleteAck.set(false); }
    }

    @Test void rejectedPrecheckSendsOnlyPrecheckNotice() throws Exception {
        Path product = SOURCE.resolve("rejected"); Files.createDirectories(product);
        Files.writeString(product.resolve("a.bin"), "payload");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.SFTP, ProtocolServers.Mode.WRITE_ONLY)) {
            EndpointForm form = server.form(EndpointRevision.Protocol.SFTP); form.localSourceRef = "source";
            String endpointId = endpoints.save(null, form);
            transferSettings.getCallers().get("push").getEndpointIds().add(endpointId);
            String request = "{\"taskId\":\"rejected\",\"direction\":\"PUSH\",\"endpointId\":\"" + endpointId
                    + "\",\"sourceCode\":\"weather\",\"sourceRoot\":\"source\",\"targetPath\":\"deliver\","
                    + "\"products\":[{\"productId\":\"p1\",\"productPath\":\"rejected\"}]}";
            var accepted = tasks.accept("push", tasks.parseJson(request), request, "JSON");
            await(() -> {
                worker.poll(); notifications.poll();
                return "REJECTED".equals(tasks.get("push", accepted.taskRecordId).executionStatus);
            });
            assertThat(db.queryForObject("SELECT count(*) FROM transfer_notification WHERE task_id=? AND stage='PRECHECK'", Integer.class, accepted.taskRecordId)).isEqualTo(1);
            assertThat(db.queryForObject("SELECT count(*) FROM transfer_notification WHERE task_id=? AND stage='COMPLETE'", Integer.class, accepted.taskRecordId)).isZero();
            assertThat(Files.exists(remoteRoot.resolve("deliver/weather/" + accepted.taskRecordId))).isFalse();
        }
    }

    private static void await(java.util.concurrent.Callable<Boolean> condition) throws Exception {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(20);
        while (System.nanoTime() < deadline) {
            try { if (condition.call()) return; }
            catch (org.springframework.dao.EmptyResultDataAccessException ignored) { }
            Thread.sleep(50);
        }
        throw new AssertionError("Timed out waiting for push state");
    }

    private Task task(String endpointId, String revisionId) throws Exception {
        Task task = new Task(); task.taskRecordId = UUID.randomUUID().toString(); task.taskId = task.taskRecordId;
        task.callerId = "test"; task.entryType = "JSON"; task.direction = "PUSH"; task.endpointId = endpointId;
        task.endpointRevisionId = revisionId; task.phase = "ACCEPTED";
        task.canonicalRequest = new ObjectMapper().createObjectNode().put("targetPath", "deliver")
                .put("sourceCode", "weather").toString();
        task.products = List.of(Map.of("product_id", "p1", "product_path", "one"),
                Map.of("product_id", "p2", "product_path", "two"));
        db.update("INSERT INTO transfer_task (id,caller_id,external_task_id,entry_type,direction,endpoint_id,endpoint_revision_id,notification_url,canonical_request,raw_request,push_source_root,status,phase,next_run_at,claimed_at,created_at,updated_at) VALUES (?,?,?,?,?,?,?,'http://127.0.0.1/notify',?,?,?,'RUNNING','ACCEPTED',now(),now(),now(),now())",
                task.taskRecordId, task.callerId, task.taskId, task.entryType, task.direction, task.endpointId,
                task.endpointRevisionId, task.canonicalRequest, task.canonicalRequest, SOURCE.toRealPath().toString());
        return task;
    }

    @AfterAll static void cleanup() throws Exception {
        if (receiver != null) receiver.stop(0);
        if (!SCHEMA.matches("push_it_[a-f0-9]{32}")) throw new IllegalStateException("Unsafe schema cleanup");
        try (var connection = DriverManager.getConnection(URL, USER, PASSWORD); var statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE");
        }
        try (var paths = Files.walk(SOURCE)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> { try { Files.delete(path); } catch (java.io.IOException error) { throw new java.io.UncheckedIOException(error); } });
        }
    }
}
