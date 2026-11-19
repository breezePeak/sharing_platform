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
    // Independent SQL is retained for scheduler/ACK fault injection and database assertions.
    @Autowired JdbcTemplate db;
    @Autowired TaskService tasks;
    @Autowired TaskWorker worker;
    @Autowired NotificationDispatcher notifications;
    @Autowired TransferSettings transferSettings;
    @Autowired RemoteFileClientFactory clients;
    @Autowired ObjectMapper json;
    @Autowired PushStore store;
    @Autowired TransferTaskRepository taskRecords;
    @Autowired TransferFileRepository fileRecords;
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

    @ParameterizedTest @EnumSource(EndpointRevision.Protocol.class)
    void sameSizeCorruptTargetNeverPublishes(EndpointRevision.Protocol protocol) throws Exception {
        Path product = SOURCE.resolve("corrupt"); Files.createDirectories(product);
        Files.writeString(product.resolve("a.bin"), "original");
        Path other = SOURCE.resolve("corrupt-other"); Files.createDirectories(other);
        Files.writeString(other.resolve("b.bin"), "original");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, protocol, ProtocolServers.Mode.NORMAL)) {
            EndpointForm form = server.form(protocol); form.localSourceRef = "source";
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

    @ParameterizedTest @EnumSource(EndpointRevision.Protocol.class)
    void readyRenameSucceededButReplyLostReconcilesWithoutReupload(EndpointRevision.Protocol protocol) throws Exception {
        Path product = SOURCE.resolve("lost-reply"); Files.createDirectories(product);
        Files.writeString(product.resolve("a.bin"), "fixed content");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, protocol, ProtocolServers.Mode.NORMAL)) {
            EndpointForm form = server.form(protocol); form.localSourceRef = "source";
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
            PushProcessor interrupted = new PushProcessor(store, json, revisions, transferSettings, faulty);
            assertThatThrownBy(() -> interrupted.process(task)).isInstanceOf(java.io.IOException.class);
            Path delivered = remoteRoot.resolve("deliver/weather/" + task.taskRecordId);
            assertThat(Files.readString(delivered.resolve("_READY"))).contains("taskId=" + task.taskRecordId);
            var modified = Files.getLastModifiedTime(delivered.resolve("p1/a.bin"));
            assertThat(push.process(task).status).isEqualTo("SUCCEEDED");
            assertThat(Files.getLastModifiedTime(delivered.resolve("p1/a.bin"))).isEqualTo(modified);
        }
    }

    @ParameterizedTest @EnumSource(EndpointRevision.Protocol.class)
    void interruptedFileStreamRestartsSameFileAndPublishesOnce(EndpointRevision.Protocol protocol) throws Exception {
        Path product = SOURCE.resolve("interrupted"); Files.createDirectories(product);
        Files.writeString(product.resolve("a.bin"), "long enough to break midstream");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, protocol, ProtocolServers.Mode.NORMAL)) {
            EndpointForm form = server.form(protocol); form.localSourceRef = "source";
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
            PushProcessor interrupted = new PushProcessor(store, json, revisions, transferSettings, faulty);
            assertThatThrownBy(() -> interrupted.process(task)).isInstanceOf(java.io.IOException.class);
            assertThat(push.process(task).status).isEqualTo("SUCCEEDED");
            Path delivered = remoteRoot.resolve("deliver/weather/" + task.taskRecordId);
            assertThat(Files.readString(delivered.resolve("p1/a.bin"))).isEqualTo("long enough to break midstream");
            Task visible = tasks.get("test", task.taskRecordId);
            assertThat(visible.uploadedBytes).isEqualTo(visible.totalBytes);
            assertThat(visible.uploadTrafficBytes).isGreaterThan(visible.totalBytes);
            assertThat(visible.retransmittedBytes).isEqualTo(4);
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
            assertThat(tasks.get("push", accepted.taskRecordId).phase).isEqualTo("NOTIFYING");
            refuseCompleteAck.set(false);
            db.update("UPDATE transfer_notification SET next_attempt_at=now() WHERE task_id=? AND stage='COMPLETE'", accepted.taskRecordId);
            await(() -> {
                notifications.poll();
                return "ACKED".equals(db.queryForObject("SELECT status FROM transfer_notification WHERE task_id=? AND stage='COMPLETE'", String.class, accepted.taskRecordId));
            });
            assertThat(Files.getLastModifiedTime(deliveredFile)).isEqualTo(modified);
            assertThat(tasks.get("push", accepted.taskRecordId).executionStatus).isEqualTo("SUCCEEDED");
            assertThat(tasks.get("push", accepted.taskRecordId).phase).isEqualTo("COMPLETE");
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

    @Test void frozenFilesAndTaskSnapshotRollBackTogetherOnDatabaseFailure() throws Exception {
        String name = "freeze-rollback-" + UUID.randomUUID(); Files.createDirectories(SOURCE.resolve(name + "/nested"));
        Files.writeString(SOURCE.resolve(name + "/nested/a.bin"), "payload");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            EndpointForm form = server.form(EndpointRevision.Protocol.FTP); form.localSourceRef = "source";
            String endpointId = endpoints.save(null, form);
            Task task = task(endpointId, revisions.findByEndpointIdAndRevision(endpointId, 1).orElseThrow().id);
            task.products = List.of(Map.of("product_id", "p1", "product_path", name));
            db.execute("CREATE FUNCTION fail_push_freeze() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.task_id='" + task.taskRecordId + "' THEN RAISE EXCEPTION 'forced'; END IF; RETURN NEW; END $$");
            db.execute("CREATE TRIGGER fail_push_freeze BEFORE INSERT ON transfer_file FOR EACH ROW EXECUTE FUNCTION fail_push_freeze()");
            try {
                assertThatThrownBy(() -> push.process(task)).isInstanceOf(RuntimeException.class);
                assertThat(fileRecords.countByTaskId(task.taskRecordId)).isZero();
                assertThat(store.directories(task.taskRecordId)).isEmpty();
                assertThat(taskRecords.findById(task.taskRecordId).orElseThrow().pushManifestSha256).isNull();
            } finally { db.execute("DROP TRIGGER fail_push_freeze ON transfer_file"); db.execute("DROP FUNCTION fail_push_freeze()"); }
            assertThat(push.process(task).status).isEqualTo("WAITING_FEEDBACK");
            assertThat(fileRecords.countByTaskId(task.taskRecordId)).isEqualTo(1);
        }
    }

    @Test void confirmedPublicationRecoversEvenAfterLocalSourceRootIsRemoved() throws Exception {
        Path isolated = Files.createDirectory(SOURCE.resolve("published-" + UUID.randomUUID()));
        Path product = Files.createDirectory(isolated.resolve("product")); Files.writeString(product.resolve("a.bin"), "fixed");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            EndpointForm form = server.form(EndpointRevision.Protocol.FTP); form.localSourceRef = "source";
            String endpointId = endpoints.save(null, form);
            Task task = task(endpointId, revisions.findByEndpointIdAndRevision(endpointId, 1).orElseThrow().id);
            TransferTaskRecord record = taskRecords.findById(task.taskRecordId).orElseThrow();
            record.pushSourceRoot = isolated.toRealPath().toString(); taskRecords.saveAndFlush(record);
            task.products = List.of(Map.of("product_id", "p1", "product_path", "product"));
            assertThat(push.process(task).status).isEqualTo("WAITING_FEEDBACK"); task.phase = "PRECHECK_ACKED";
            assertThat(push.process(task).status).isEqualTo("SUCCEEDED");
            Files.delete(product.resolve("a.bin")); Files.delete(product); Files.delete(isolated);
            assertThat(push.process(task).status).isEqualTo("SUCCEEDED");
        }
    }

    @Test void uploadAndReadbackCheckpointsAreVisibleBeforeTheFileFinishes() throws Exception {
        String sourceName = "progress-" + UUID.randomUUID();
        Files.createDirectories(SOURCE.resolve(sourceName));
        Files.write(SOURCE.resolve(sourceName + "/large.bin"), new byte[10 * 1024 * 1024]);
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            EndpointForm form = server.form(EndpointRevision.Protocol.FTP); form.localSourceRef = "source";
            String endpointId = endpoints.save(null, form);
            Task task = task(endpointId, revisions.findByEndpointIdAndRevision(endpointId, 1).orElseThrow().id);
            task.products = List.of(Map.of("product_id", "p1", "product_path", sourceName));
            assertThat(push.process(task).status).isEqualTo("WAITING_FEEDBACK"); task.phase = "PRECHECK_ACKED";
            AtomicBoolean uploaded = new AtomicBoolean(), readback = new AtomicBoolean();
            RemoteFileClientFactory observed = mock(RemoteFileClientFactory.class);
            when(observed.connect(any(EndpointRevision.class))).thenAnswer(invocation -> {
                RemoteFileClient real = clients.connect(invocation.getArgument(0));
                return java.lang.reflect.Proxy.newProxyInstance(RemoteFileClient.class.getClassLoader(), new Class<?>[] {RemoteFileClient.class},
                        (proxy, method, arguments) -> {
                    try {
                        if (method.getName().equals("write") && ((String) arguments[0]).contains(".sharing-file-")) {
                            arguments[1] = new java.io.FilterInputStream((java.io.InputStream) arguments[1]) {
                                long count;
                                @Override public int read(byte[] bytes, int offset, int length) throws java.io.IOException {
                                    if (count >= 8 * 1024 * 1024 && uploaded.compareAndSet(false, true)) {
                                        Task visible = tasks.get("test", task.taskRecordId);
                                        assertThat(visible.uploadedBytes).isPositive().isLessThan(visible.totalBytes);
                                        assertThat(visible.phase).isEqualTo("TRANSFERRING"); assertThat(visible.verifiedBytes).isZero();
                                    }
                                    int n = in.read(bytes, offset, length); if (n > 0) count += n; return n;
                                }
                            };
                        }
                        if (method.getName().equals("read") && ((String) arguments[0]).contains(".sharing-file-")) {
                            arguments[1] = new java.io.FilterOutputStream((java.io.OutputStream) arguments[1]) {
                                long count;
                                @Override public void write(byte[] bytes, int offset, int length) throws java.io.IOException {
                                    out.write(bytes, offset, length); count += length;
                                    if (count >= 8 * 1024 * 1024 && readback.compareAndSet(false, true)) {
                                        Task visible = tasks.get("test", task.taskRecordId);
                                        assertThat(visible.readbackBytes).isPositive(); assertThat(visible.uploadedBytes).isEqualTo(visible.totalBytes);
                                        assertThat(visible.phase).isEqualTo("VERIFYING"); assertThat(visible.executionStatus).isEqualTo("RUNNING");
                                    }
                                }
                            };
                        }
                        return method.invoke(real, arguments);
                    } catch (java.lang.reflect.InvocationTargetException error) { throw error.getCause(); }
                });
            });
            assertThat(new PushProcessor(store, json, revisions, transferSettings, observed).process(task).status).isEqualTo("SUCCEEDED");
            Task visible = tasks.get("test", task.taskRecordId);
            assertThat(uploaded).isTrue(); assertThat(readback).isTrue();
            assertThat(visible.uploadedBytes).isEqualTo(visible.totalBytes);
            assertThat(visible.uploadTrafficBytes).isEqualTo(visible.totalBytes);
            assertThat(visible.verifiedBytes).isEqualTo(visible.totalBytes);
            assertThat(visible.retransmittedBytes).isZero();
            assertThat(visible.readbackBytes).isGreaterThanOrEqualTo(visible.totalBytes);
        }
    }

    @ParameterizedTest @EnumSource(EndpointRevision.Protocol.class)
    void manualRecheckUsesExistingPublicationEvidenceWithoutWriting(EndpointRevision.Protocol protocol) throws Exception {
        String sourceName = "recheck-" + UUID.randomUUID();
        Files.createDirectories(SOURCE.resolve(sourceName));
        Files.writeString(SOURCE.resolve(sourceName + "/a.bin"), "read-only recheck");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, protocol, ProtocolServers.Mode.NORMAL)) {
            EndpointForm form = server.form(protocol); form.localSourceRef = "source";
            String endpointId = endpoints.save(null, form);
            Task task = task(endpointId, revisions.findByEndpointIdAndRevision(endpointId, 1).orElseThrow().id);
            task.products = List.of(Map.of("product_id", "p1", "product_path", sourceName));
            assertThat(push.process(task).status).isEqualTo("WAITING_FEEDBACK");
            task.phase = "PRECHECK_ACKED";
            assertThat(push.process(task).status).isEqualTo("SUCCEEDED");
            var record = taskRecords.findById(task.taskRecordId).orElseThrow();
            record.readOnlyRecheck = true;
            taskRecords.saveAndFlush(record);
            Path ready = remoteRoot.resolve("deliver/weather/" + task.taskRecordId + "/_READY");
            Path held = remoteRoot.resolve("held-marker");
            Files.move(ready, held);
            RemoteFileClientFactory readOnly = mock(RemoteFileClientFactory.class);
            when(readOnly.connect(any(EndpointRevision.class))).thenAnswer(invocation -> {
                RemoteFileClient real = clients.connect(invocation.getArgument(0));
                return java.lang.reflect.Proxy.newProxyInstance(RemoteFileClient.class.getClassLoader(),
                        new Class<?>[] {RemoteFileClient.class}, (proxy, method, args) -> {
                            if (Set.of("write", "mkdir", "rename", "deleteTemporary").contains(method.getName()))
                                throw new AssertionError("Manual recheck tried to modify the target");
                            try { return method.invoke(real, args); }
                            catch (java.lang.reflect.InvocationTargetException error) { throw error.getCause(); }
                        });
            });
            PushProcessor recheck = new PushProcessor(store, json, revisions, transferSettings, readOnly);
            assertThat(recheck.process(task).status).isEqualTo("RECONCILING");
            assertThat(ready).doesNotExist();
            Files.move(held, ready);
            assertThat(recheck.process(task).status).isEqualTo("SUCCEEDED");
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({"FTP,ready,false", "FTP,ready,true", "FTP,manifest,false", "FTP,manifest,true", "SFTP,ready,false", "SFTP,ready,true", "SFTP,manifest,false", "SFTP,manifest,true"})
    void interruptedControlFilesRecoverWithoutReupload(EndpointRevision.Protocol protocol, String control, boolean partial) throws Exception {
        String sourceName = "control-" + UUID.randomUUID();
        Files.createDirectories(SOURCE.resolve(sourceName));
        Files.writeString(SOURCE.resolve(sourceName + "/a.bin"), "fixed content");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, protocol, ProtocolServers.Mode.NORMAL)) {
            EndpointForm form = server.form(protocol); form.localSourceRef = "source";
            String endpointId = endpoints.save(null, form);
            Task task = task(endpointId, revisions.findByEndpointIdAndRevision(endpointId, 1).orElseThrow().id);
            task.products = List.of(Map.of("product_id", "p1", "product_path", sourceName));
            assertThat(push.process(task).status).isEqualTo("WAITING_FEEDBACK"); task.phase = "PRECHECK_ACKED";
            RemoteFileClientFactory faulty = mock(RemoteFileClientFactory.class);
            when(faulty.connect(any(EndpointRevision.class))).thenAnswer(invocation -> {
                RemoteFileClient real = clients.connect(invocation.getArgument(0));
                return java.lang.reflect.Proxy.newProxyInstance(RemoteFileClient.class.getClassLoader(),
                        new Class<?>[] {RemoteFileClient.class}, (proxy, method, arguments) -> {
                            try {
                                if (method.getName().equals("write") && ((String) arguments[0]).contains("/.sharing-" + control + "-")) {
                                    byte[] bytes = ((java.io.InputStream) arguments[1]).readAllBytes();
                                    real.write((String) arguments[0], new java.io.ByteArrayInputStream(partial ? Arrays.copyOf(bytes, bytes.length / 2) : bytes));
                                    real.close(); throw new java.io.IOException("INTERRUPTED_CONTROL_WRITE");
                                }
                                return method.invoke(real, arguments);
                            } catch (java.lang.reflect.InvocationTargetException error) { throw error.getCause(); }
                        });
            });
            PushProcessor interrupted = new PushProcessor(store, json, revisions, transferSettings, faulty);
            assertThatThrownBy(() -> interrupted.process(task)).isInstanceOf(java.io.IOException.class);
            Path delivered = remoteRoot.resolve("deliver/weather/" + task.taskRecordId);
            Path temporary = delivered.resolve(".sharing-" + control + "-" + task.taskRecordId);
            assertThat(Files.exists(temporary)).isTrue();
            var modified = Files.getLastModifiedTime(delivered.resolve("p1/a.bin"));
            assertThat(push.process(task).status).isEqualTo("SUCCEEDED");
            assertThat(Files.exists(temporary)).isFalse();
            assertThat(Files.getLastModifiedTime(delivered.resolve("p1/a.bin"))).isEqualTo(modified);
            assertThat(Files.readString(delivered.resolve("_manifest.xml"))).isEqualTo(taskRecords.findById(task.taskRecordId).orElseThrow().pushManifest);
        }
    }

    @ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(strings = {"other-task", "own-wrong-content", "own-no-intent"})
    void unownedTemporaryFilesStillBlockPublication(String mode) throws Exception {
        String name = "foreign-" + UUID.randomUUID();
        Files.createDirectories(SOURCE.resolve(name)); Files.writeString(SOURCE.resolve(name + "/a.bin"), "payload");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            EndpointForm form = server.form(EndpointRevision.Protocol.FTP); form.localSourceRef = "source";
            String endpointId = endpoints.save(null, form);
            Task task = task(endpointId, revisions.findByEndpointIdAndRevision(endpointId, 1).orElseThrow().id);
            task.products = List.of(Map.of("product_id", "p1", "product_path", name));
            assertThat(push.process(task).status).isEqualTo("WAITING_FEEDBACK"); task.phase = "PRECHECK_ACKED";
            Path delivered = remoteRoot.resolve("deliver/weather/" + task.taskRecordId); Files.createDirectories(delivered);
            String owner = mode.equals("other-task") ? UUID.randomUUID().toString() : task.taskRecordId;
            Path temporary = delivered.resolve(".sharing-manifest-" + owner); Files.writeString(temporary, "foreign");
            if (mode.equals("own-wrong-content")) store.manifestIntent(task);
            assertThatThrownBy(() -> push.process(task)).isInstanceOf(java.io.IOException.class);
            assertThat(Files.readString(temporary)).isEqualTo("foreign");
            assertThat(Files.exists(delivered.resolve("_READY"))).isFalse();
        }
    }

    @Test void databaseCollationCannotChangeFrozenManifestAndLateClaimsCannotChangeProgress() throws Exception {
        String name = "sort-" + UUID.randomUUID(); Path product = SOURCE.resolve(name); Files.createDirectories(product);
        for (String path : List.of("Z.bin", "a.bin", "10.bin", "2.bin", "中文/层/a.bin", "é.bin", "😀.bin", "中.bin")) {
            Path file = product.resolve(path); Files.createDirectories(file.getParent()); Files.writeString(file, "same length payload");
        }
        // Independent SQL assertion proves the test actually exercises different PostgreSQL sort orders.
        List<String> localeOrder = db.queryForList("SELECT name FROM (VALUES ('Z.bin'),('a.bin')) v(name) ORDER BY name COLLATE \"default\"", String.class);
        List<String> cOrder = db.queryForList("SELECT name FROM (VALUES ('Z.bin'),('a.bin')) v(name) ORDER BY name COLLATE \"C\"", String.class);
        if (localeOrder.equals(cOrder)) {
            List<String> collations = db.queryForList("SELECT collname FROM pg_collation WHERE (collname LIKE 'en_US%' OR collname LIKE 'en-US%' OR collname LIKE 'English_United States%') AND collname NOT ILIKE '%posix%' ORDER BY collname", String.class);
            assertThat(collations).as("An English PostgreSQL collation is required for this regression").isNotEmpty();
            String collation = collations.get(0).replace("\"", "\"\"");
            localeOrder = db.queryForList("SELECT name FROM (VALUES ('Z.bin'),('a.bin')) v(name) ORDER BY name COLLATE \"" + collation + "\"", String.class);
        }
        assertThat(localeOrder).isNotEqualTo(cOrder);
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            EndpointForm form = server.form(EndpointRevision.Protocol.FTP); form.localSourceRef = "source";
            String endpointId = endpoints.save(null, form);
            Task task = task(endpointId, revisions.findByEndpointIdAndRevision(endpointId, 1).orElseThrow().id);
            task.products = List.of(Map.of("product_id", "p1", "product_path", name));
            assertThat(push.process(task).status).isEqualTo("WAITING_FEEDBACK"); task.phase = "PRECHECK_ACKED";
            String frozen = taskRecords.findById(task.taskRecordId).orElseThrow().pushManifest;
            assertThat(push.process(task).status).isEqualTo("SUCCEEDED");
            assertThat(Files.readString(remoteRoot.resolve("deliver/weather/" + task.taskRecordId + "/_manifest.xml"))).isEqualTo(frozen);
            Task stale = task;
            TransferTaskRecord record = taskRecords.findById(task.taskRecordId).orElseThrow(); record.claimId = UUID.randomUUID().toString(); taskRecords.saveAndFlush(record);
            TransferFileRecord file = store.files(task.taskRecordId).get(0); long before = file.readbackBytes;
            assertThatThrownBy(() -> store.progress(stale, file, 999, 999, 999, true)).hasMessage("EXECUTION_CLAIM_LOST");
            assertThatThrownBy(() -> store.phase(stale, "PUBLISHING")).hasMessage("EXECUTION_CLAIM_LOST");
            assertThatThrownBy(() -> store.publishIntent(stale)).hasMessage("EXECUTION_CLAIM_LOST");
            assertThat(store.files(task.taskRecordId).get(0).readbackBytes).isEqualTo(before);
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
        task.claimId = UUID.randomUUID().toString();
        TransferTaskRecord record = new TransferTaskRecord();
        record.id = task.taskRecordId; record.callerId = task.callerId; record.externalTaskId = task.taskId;
        record.entryType = task.entryType; record.direction = task.direction; record.endpointId = task.endpointId;
        record.endpointRevisionId = task.endpointRevisionId; record.notificationUrl = "http://127.0.0.1/notify";
        record.canonicalRequest = task.canonicalRequest; record.rawRequest = task.canonicalRequest;
        record.pushSourceRoot = SOURCE.toRealPath().toString(); record.status = "RUNNING"; record.phase = "ACCEPTED";
        record.claimId = task.claimId; record.acceptanceStatus = "NOT_STARTED";
        record.nextRunAt = record.claimedAt = record.createdAt = record.updatedAt = java.time.Instant.now();
        taskRecords.saveAndFlush(record);
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
