package com.siwei;

import com.siwei.endpoint.CredentialCipher;
import com.siwei.endpoint.EndpointSettings;
import java.net.URI;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class ReleaseBackupIT {
    @TempDir Path root;

    @Test @EnabledIfEnvironmentVariable(named = "TEST_PG_BIN", matches = ".+")
    void restoresLogicalBackupAndMatchesTheRetainedDeliveryFilesAndCredentialVersion() throws Exception {
        String url = TestDatabase.requireUrl(System.getenv());
        String user = TestDatabase.require(System.getenv(), "TEST_DB_USERNAME");
        String password = TestDatabase.require(System.getenv(), "TEST_DB_PASSWORD");
        URI cluster = URI.create(url.substring(5));
        String source = cluster.getPath().substring(1);
        assertThat(source).endsWith("_test");
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String schema = "receipt_backup_" + suffix, restored = "restore_" + suffix + "_test";
        String endpoint = UUID.randomUUID().toString(), revision = UUID.randomUUID().toString(), task = UUID.randomUUID().toString();
        var settings = new EndpointSettings();
        byte[] testKey = new byte[32]; new java.security.SecureRandom().nextBytes(testKey);
        settings.setMasterKey(Base64.getEncoder().encodeToString(testKey));
        String encrypted = new CredentialCipher(settings).encrypt("test-only-secret", revision);
        String restoredUrl = url.substring(0, url.lastIndexOf('/') + 1) + restored;
        Path files = Files.createDirectory(root.resolve("delivery"));
        Files.writeString(files.resolve("data.bin"), "retained business file");
        Files.writeString(files.resolve(".sharing-complete"), task);
        Path fileBackup = Files.createDirectory(root.resolve("files-backup"));
        for (String name : List.of("data.bin", ".sharing-complete")) Files.copy(files.resolve(name), fileBackup.resolve(name));
        try (Connection database = DriverManager.getConnection(url, user, password)) {
            Flyway.configure().dataSource(url, user, password).defaultSchema(schema).load().migrate();
            database.setSchema(schema);
            try (Statement sql = database.createStatement()) { sql.execute("CREATE DATABASE " + restored); }
            try {
                try (PreparedStatement insert = database.prepareStatement("INSERT INTO endpoint(id,revision,enabled) VALUES (?,1,true)")) {
                    insert.setString(1, endpoint); insert.executeUpdate();
                }
                try (PreparedStatement insert = database.prepareStatement("INSERT INTO endpoint_revision(id,endpoint_id,revision,name,protocol,host,port,"
                        + "username,auth,secret,timeout_seconds,concurrency,passive,isolated_ftp_root,created_at) "
                        + "VALUES (?,?,1,'Backup test','FTP','127.0.0.1',21,'tester','PASSWORD',?,3,1,true,true,now())")) {
                    insert.setString(1, revision); insert.setString(2, endpoint); insert.setString(3, encrypted); insert.executeUpdate();
                }
                try (PreparedStatement insert = database.prepareStatement("INSERT INTO transfer_task(id,caller_id,external_task_id,entry_type,direction,"
                        + "endpoint_id,endpoint_revision_id,notification_url,canonical_request,raw_request,status,phase,acceptance_status,next_run_at,created_at,updated_at) "
                        + "VALUES (?,'test','backup','JSON','PULL',?,?,'http://127.0.0.1/fixture','{}','{}','WAITING_ACCEPTANCE','WAITING_ACCEPTANCE','PENDING',now(),now(),now())")) {
                    insert.setString(1, task); insert.setString(2, endpoint); insert.setString(3, revision); insert.executeUpdate();
                }
                Path archive = root.resolve("postgres.dump");
                run(cluster, user, password, "pg_dump", source, "-n", schema, "-Fc", "-f", archive.toString());
                run(cluster, user, password, "pg_restore", restored, "--exit-on-error", "--no-owner", archive.toString());
                try (Connection clone = DriverManager.getConnection(restoredUrl, user, password)) {
                    clone.setSchema(schema);
                    try (PreparedStatement select = clone.prepareStatement("SELECT t.status,t.acceptance_status,r.id,r.secret FROM transfer_task t "
                            + "JOIN endpoint_revision r ON r.id=t.endpoint_revision_id WHERE t.id=?")) {
                        select.setString(1, task);
                        try (ResultSet row = select.executeQuery()) {
                            assertThat(row.next()).isTrue();
                            assertThat(row.getString(1)).isEqualTo("WAITING_ACCEPTANCE");
                            assertThat(row.getString(2)).isEqualTo("PENDING");
                            assertThat(row.getString(3)).isEqualTo(revision);
                            assertThat(row.getString(4)).isEqualTo(encrypted);
                            var restoredRevision = new com.siwei.endpoint.EndpointRevision();
                            restoredRevision.id = revision;
                            org.springframework.test.util.ReflectionTestUtils.setField(restoredRevision, "secret", row.getString(4));
                            assertThat(new CredentialCipher(settings).decrypt(restoredRevision)).isEqualTo("test-only-secret");
                        }
                    }
                    for (String name : List.of("data.bin", ".sharing-complete"))
                        assertThat(Files.readAllBytes(files.resolve(name))).isEqualTo(Files.readAllBytes(fileBackup.resolve(name)));
                    assertThat(Files.readString(fileBackup.resolve(".sharing-complete"))).isEqualTo(task);
                    assertThat(Flyway.configure().dataSource(restoredUrl, user, password).defaultSchema(schema).load().migrate().migrationsExecuted).isZero();
                    System.out.printf("RELEASE_BACKUP Pg=%s JDBC=%s Flyway=%s restored=%s%n", clone.getMetaData().getDatabaseProductVersion(),
                            clone.getMetaData().getDriverVersion(), Flyway.class.getPackage().getImplementationVersion(), "verified");
                }
            } finally {
                if (!restored.matches("restore_[a-f0-9]{32}_test") || !schema.matches("receipt_backup_[a-f0-9]{32}"))
                    throw new IllegalStateException("Unsafe backup fixture names");
                try (Statement sql = database.createStatement()) {
                    sql.execute("DROP DATABASE " + restored);
                    sql.execute("DROP SCHEMA " + schema + " CASCADE");
                }
            }
        }
    }

    private void run(URI cluster, String user, String password, String utility, String database, String... args) throws Exception {
        String executable = utility + (System.getProperty("os.name").startsWith("Windows") ? ".exe" : "");
        var command = new ArrayList<>(List.of(Path.of(System.getenv("TEST_PG_BIN"), executable).toString(),
                "-h", cluster.getHost(), "-p", Integer.toString(cluster.getPort() == -1 ? 5432 : cluster.getPort()), "-U", user, "-d", database));
        command.addAll(Arrays.asList(args));
        Path log = root.resolve(utility + ".log");
        ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().put("PGPASSWORD", password);
        Process child = builder.start();
        try {
            assertThat(child.waitFor(45, TimeUnit.SECONDS)).isTrue();
            assertThat(child.exitValue()).withFailMessage(() -> "PostgreSQL utility failed: " + utility).isZero();
        } finally { if (child.isAlive()) child.destroyForcibly(); }
    }
}
