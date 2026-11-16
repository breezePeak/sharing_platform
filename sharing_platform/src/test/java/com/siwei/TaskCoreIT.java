package com.siwei;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.siwei.transfer.*;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@ActiveProfiles("test")
@AutoConfigureMockMvc
@DirtiesContext
class TaskCoreIT {
    private static final String URL = TestDatabase.requireUrl(System.getenv());
    private static final String USER = TestDatabase.require(System.getenv(), "TEST_DB_USERNAME");
    private static final String PASSWORD = TestDatabase.require(System.getenv(), "TEST_DB_PASSWORD");
    private static final String SCHEMA = "task_it_" + UUID.randomUUID().toString().replace("-", "");
    private static final String ENDPOINT = UUID.randomUUID().toString();
    private static final String OUTSIDER_ENDPOINT = UUID.randomUUID().toString();
    private static final ConcurrentMap<String, AtomicInteger> executionsByTask = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, AtomicInteger> receivedByTask = new ConcurrentHashMap<>();
    private static final AtomicInteger blockedStarted = new AtomicInteger();
    private static volatile CountDownLatch blockRelease = new CountDownLatch(0);
    private static final List<String> payloads = new CopyOnWriteArrayList<>();
    private static volatile boolean wrongAck;
    private static HttpServer receiver;

    private static synchronized int receiverPort() {
        if (receiver == null) try {
            receiver = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            receiver.createContext("/notify", exchange -> {
                String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                payloads.add(body);
                var node = new ObjectMapper().readTree(body);
                String id = wrongAck || node.path("taskId").asText().equals("attention") ? "wrong" : node.path("notificationId").asText();
                String ack = "{\"accepted\":true,\"notificationId\":\"" + id
                        + "\",\"taskRecordId\":\"" + node.path("taskRecordId").asText() + "\"}";
                byte[] bytes = ack.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, bytes.length);
                try (var out = exchange.getResponseBody()) { out.write(bytes); }
                receivedByTask.computeIfAbsent(node.path("taskRecordId").asText(), ignored -> new AtomicInteger()).incrementAndGet();
            });
            receiver.start();
        } catch (java.io.IOException e) { throw new IllegalStateException(e); }
        return receiver.getAddress().getPort();
    }

    @DynamicPropertySource static void properties(DynamicPropertyRegistry p) {
        p.add("spring.datasource.url", () -> URL); p.add("spring.datasource.username", () -> USER);
        p.add("spring.datasource.password", () -> PASSWORD); p.add("spring.datasource.hikari.schema", () -> SCHEMA);
        p.add("spring.flyway.default-schema", () -> SCHEMA);
        p.add("platform.transfers.callers.test.token", () -> "test-secret");
        p.add("platform.transfers.callers.test.endpoint-ids[0]", () -> ENDPOINT);
        p.add("platform.transfers.callers.test.notification-url", () -> "http://127.0.0.1:" + receiverPort() + "/notify");
        p.add("platform.transfers.callers.test.xml-routes.weather", () -> ENDPOINT);
        p.add("platform.transfers.callers.outsider.token", () -> "other-secret");
        p.add("platform.transfers.callers.outsider.endpoint-ids[0]", () -> OUTSIDER_ENDPOINT);
        p.add("platform.transfers.provisional-xml-enabled", () -> true);
    }

    @TestConfiguration static class ProcessorConfig {
        @Bean TaskProcessor testProcessor() {
            return new TaskProcessor() {
                public String direction() { return "PUSH"; }
                public Result process(Task task) {
                    executionsByTask.computeIfAbsent(task.taskId, ignored -> new AtomicInteger()).incrementAndGet();
                    if (task.taskId.startsWith("block-") || task.taskId.equals("stale")) {
                        blockedStarted.incrementAndGet();
                        try { blockRelease.await(15, TimeUnit.SECONDS); }
                        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
                    }
                    if (task.taskId.equals("precheck") && task.phase.equals("ACCEPTED"))
                        return new Result("WAITING_FEEDBACK", "PRECHECK", null, null);
                    return new Result("SUCCEEDED", "COMPLETE", null, null);
                }
            };
        }
    }

    @Autowired TaskService service;
    @Autowired TaskWorker worker;
    @Autowired JdbcTemplate db;
    @Autowired MockMvc mvc;

    @BeforeEach void endpoint() {
        db.update("INSERT INTO endpoint (id,revision,enabled) VALUES (?,1,true) ON CONFLICT (id) DO NOTHING", ENDPOINT);
        db.update("UPDATE endpoint SET revision=1 WHERE id=?", ENDPOINT);
        db.update("INSERT INTO endpoint_revision (id,endpoint_id,revision,name,protocol,host,port,username,auth,read_root,write_root,local_source_ref,local_target_ref,timeout_seconds,concurrency,passive,isolated_ftp_root,created_at) VALUES (?,?,1,'test','FTP','127.0.0.1',21,'test','PASSWORD','/in','/out','local','local',5,1,true,true,now()) ON CONFLICT (endpoint_id,revision) DO NOTHING",
                UUID.randomUUID().toString(), ENDPOINT);
    }

    private String request(String id) {
        return "{\"taskId\":\"" + id + "\",\"direction\":\"PUSH\",\"endpointId\":\"" + ENDPOINT
                + "\",\"sourceRoot\":\"local\",\"targetPath\":\"output\",\"products\":[{\"productId\":\"p\",\"productPath\":\"p\"}]}";
    }

    @Test void concurrentIdempotencyAndConflictsStayInOneTask() throws Exception {
        String body = request("concurrent");
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<TaskService.Accepted>> results = new ArrayList<>();
            for (int i = 0; i < 8; i++) results.add(pool.submit(() -> service.accept("test", service.parseJson(body), body, "JSON")));
            Set<String> ids = new HashSet<>(); int newCount = 0;
            for (Future<TaskService.Accepted> future : results) {
                var accepted = future.get(10, TimeUnit.SECONDS); ids.add(accepted.taskRecordId);
                if (!accepted.reused) newCount++;
            }
            assertThat(ids).hasSize(1); assertThat(newCount).isEqualTo(1);
            assertThat(db.queryForObject("SELECT count(*) FROM transfer_task WHERE external_task_id='concurrent'", Integer.class)).isEqualTo(1);
            String changed = body.replace("output", "different");
            assertThatThrownBy(() -> service.accept("test", service.parseJson(changed), changed, "JSON"))
                    .isInstanceOf(org.springframework.web.server.ResponseStatusException.class);
            await(() -> executionsByTask.getOrDefault("concurrent", new AtomicInteger()).get() == 1);
            assertThat(db.queryForObject("SELECT count(*) FROM transfer_notification WHERE task_id=?", Integer.class, ids.iterator().next())).isEqualTo(1);
            String id = ids.iterator().next();
            await(() -> "ACKED".equals(notificationStatus(id)));
        } finally { pool.shutdownNow(); }
    }

    @Test void wrongAckRetriesSameBytesWithoutReexecuting() throws Exception {
        wrongAck = true; payloads.clear();
        String body = request("retry");
        var accepted = service.accept("test", service.parseJson(body), body, "JSON");
        await(() -> receivedByTask.getOrDefault(accepted.taskRecordId, new AtomicInteger()).get() >= 1);
        int before = executionsByTask.get("retry").get();
        wrongAck = false;
        await(() -> "ACKED".equals(notificationStatus(accepted.taskRecordId)));
        List<String> sameTaskPayloads = new ArrayList<>();
        for (String payload : payloads) if (payload.contains(accepted.taskRecordId)) sameTaskPayloads.add(payload);
        assertThat(sameTaskPayloads).hasSizeGreaterThanOrEqualTo(2).containsOnly(sameTaskPayloads.get(0));
        assertThat(executionsByTask.get("retry").get()).isEqualTo(before);
    }

    @Test void authenticationAndQueryScopeAreEnforced() throws Exception {
        mvc.perform(post("/api/transfers/tasks").contentType("application/json").content(request("unauth")))
                .andExpect(status().isUnauthorized());
        mvc.perform(get("/api/transfers/tasks/" + UUID.randomUUID()).header("Authorization", "Bearer test-secret"))
                .andExpect(status().isNotFound());
        String body = request("private");
        var accepted = service.accept("test", service.parseJson(body), body, "JSON");
        mvc.perform(get("/api/transfers/tasks/" + accepted.taskRecordId).header("Authorization", "Bearer other-secret"))
                .andExpect(status().isNotFound());
        assertThat(db.queryForObject("SELECT count(*) FROM transfer_task WHERE external_task_id='unauth'", Integer.class)).isZero();
    }

    @Test void equivalentXmlAndJsonReuseOneTaskAndXxeDoesNotReachDatabase() {
        String body = request("cross-format").replace("\"sourceRoot\"", "\"sourceCode\":\"weather\",\"sourceRoot\"");
        var first = service.accept("test", service.parseJson(body), body, "JSON");
        String xml = "<taskNotice><taskId>cross-format</taskId><type>weather</type><sourceRoot>local</sourceRoot>"
                + "<targetPath>output</targetPath><fileCount>1</fileCount><ProductList><product>"
                + "<productId>p</productId><productPath>p</productPath><productName></productName>"
                + "</product></ProductList></taskNotice>";
        var repeat = service.accept("test", service.parseXml(xml), xml, "XML");
        assertThat(repeat.reused).isTrue(); assertThat(repeat.taskRecordId).isEqualTo(first.taskRecordId);
        assertThatThrownBy(() -> service.parseXml("<!DOCTYPE taskNotice [<!ENTITY xxe SYSTEM 'file:///etc/passwd'>]>" + xml))
                .isInstanceOf(org.springframework.web.server.ResponseStatusException.class);
        assertThat(db.queryForObject("SELECT count(*) FROM transfer_task WHERE external_task_id='cross-format'", Integer.class)).isEqualTo(1);
    }

    @Test void precheckAckResumesTheSameTaskAndKeepsBothNotifications() throws Exception {
        String body = request("precheck");
        var accepted = service.accept("test", service.parseJson(body), body, "JSON");
        await(() -> db.queryForObject("SELECT count(*) FROM transfer_notification WHERE task_id=? AND status='ACKED'", Integer.class, accepted.taskRecordId) == 2);
        assertThat(db.queryForObject("SELECT status FROM transfer_task WHERE id=?", String.class, accepted.taskRecordId)).isEqualTo("SUCCEEDED");
        assertThat(db.queryForObject("SELECT count(DISTINCT stage) FROM transfer_notification WHERE task_id=?", Integer.class, accepted.taskRecordId)).isEqualTo(2);
    }

    @Test void replayAfterEndpointEditKeepsOriginalRevision() {
        String body = request("revision-stable");
        var first = service.accept("test", service.parseJson(body), body, "JSON");
        String original = db.queryForObject("SELECT endpoint_revision_id FROM transfer_task WHERE id=?", String.class, first.taskRecordId);
        db.update("INSERT INTO endpoint_revision (id,endpoint_id,revision,name,protocol,host,port,username,auth,read_root,write_root,local_source_ref,local_target_ref,timeout_seconds,concurrency,passive,isolated_ftp_root,created_at) VALUES (?,?,2,'updated','FTP','127.0.0.2',21,'test','PASSWORD','/in','/changed','local','local',5,1,true,true,now())",
                UUID.randomUUID().toString(), ENDPOINT);
        db.update("UPDATE endpoint SET revision=2 WHERE id=?", ENDPOINT);
        var replay = service.accept("test", service.parseJson(body), body, "JSON");
        assertThat(replay.reused).isTrue(); assertThat(replay.taskRecordId).isEqualTo(first.taskRecordId);
        assertThat(db.queryForObject("SELECT endpoint_revision_id FROM transfer_task WHERE id=?", String.class, first.taskRecordId)).isEqualTo(original);
    }

    @Test void insertFailureRollsBackTaskAndProductTogether() {
        db.execute("CREATE FUNCTION fail_product() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.product_id='rollback' THEN RAISE EXCEPTION 'forced'; END IF; RETURN NEW; END $$");
        db.execute("CREATE TRIGGER fail_product_insert BEFORE INSERT ON transfer_product FOR EACH ROW EXECUTE FUNCTION fail_product()");
        try {
            String body = request("rollback").replace("\"productId\":\"p\"", "\"productId\":\"rollback\"");
            assertThatThrownBy(() -> service.accept("test", service.parseJson(body), body, "JSON"))
                    .isInstanceOf(org.springframework.dao.DataAccessException.class);
            assertThat(db.queryForObject("SELECT count(*) FROM transfer_task WHERE external_task_id='rollback'", Integer.class)).isZero();
            assertThat(db.queryForObject("SELECT count(*) FROM transfer_notification n JOIN transfer_task t ON n.task_id=t.id WHERE t.external_task_id='rollback'", Integer.class)).isZero();
        } finally {
            db.execute("DROP TRIGGER fail_product_insert ON transfer_product");
            db.execute("DROP FUNCTION fail_product()");
        }
    }

    @Test void fullWorkerPoolLeavesThirdTaskDurablyQueued() throws Exception {
        blockedStarted.set(0); blockRelease = new CountDownLatch(1);
        try {
            for (int i = 1; i <= 3; i++) {
                String body = request("block-" + i);
                service.accept("test", service.parseJson(body), body, "JSON");
            }
            await(() -> blockedStarted.get() == 2);
            assertThat(db.queryForObject("SELECT count(*) FROM transfer_task WHERE external_task_id LIKE 'block-%' AND status='QUEUED'", Integer.class)).isEqualTo(1);
        } finally { blockRelease.countDown(); }
        await(() -> db.queryForObject("SELECT count(*) FROM transfer_task WHERE external_task_id LIKE 'block-%' AND status='SUCCEEDED'", Integer.class) == 3);
    }

    @Test void wrongAckStopsAtConfiguredAttemptLimit() throws Exception {
        String body = request("attention");
        var accepted = service.accept("test", service.parseJson(body), body, "JSON");
        await(() -> notificationStatus(accepted.taskRecordId) != null);
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (!"NEEDS_ATTENTION".equals(notificationStatus(accepted.taskRecordId)) && System.nanoTime() < deadline) {
            db.update("UPDATE transfer_notification SET next_attempt_at=now() WHERE task_id=? AND status='RETRY_WAIT'", accepted.taskRecordId);
            Thread.sleep(100);
        }
        assertThat(notificationStatus(accepted.taskRecordId)).isEqualTo("NEEDS_ATTENTION");
        assertThat(db.queryForObject("SELECT attempts FROM transfer_notification WHERE task_id=?", Integer.class, accepted.taskRecordId)).isEqualTo(5);
    }

    @Test void staleClaimCannotOverwriteNewResult() throws Exception {
        blockedStarted.set(0); blockRelease = new CountDownLatch(1);
        String body = request("stale");
        var accepted = service.accept("test", service.parseJson(body), body, "JSON");
        try {
            await(() -> blockedStarted.get() == 1);
            db.update("UPDATE transfer_task SET claimed_at=now()-interval '6 minutes' WHERE id=?", accepted.taskRecordId);
            worker.poll();
            await(() -> blockedStarted.get() == 2);
        } finally { blockRelease.countDown(); }
        await(() -> "SUCCEEDED".equals(db.queryForObject("SELECT status FROM transfer_task WHERE id=?", String.class, accepted.taskRecordId)));
        assertThat(db.queryForObject("SELECT count(*) FROM transfer_notification WHERE task_id=?", Integer.class, accepted.taskRecordId)).isEqualTo(1);
    }

    @Test @org.junit.jupiter.api.extension.ExtendWith(OutputCaptureExtension.class)
    void failedResultTransactionLeavesTaskClaimAndNotificationTogether(CapturedOutput output) throws Exception {
        db.execute("CREATE FUNCTION fail_notification() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF EXISTS (SELECT 1 FROM transfer_task WHERE id=NEW.task_id AND external_task_id='result-rollback') THEN RAISE EXCEPTION 'forced'; END IF; RETURN NEW; END $$");
        db.execute("CREATE TRIGGER fail_notification_insert BEFORE INSERT ON transfer_notification FOR EACH ROW EXECUTE FUNCTION fail_notification()");
        String body = request("result-rollback");
        var accepted = service.accept("test", service.parseJson(body), body, "JSON");
        try {
            await(() -> output.toString().contains("task result persistence failed id=" + accepted.taskRecordId));
            assertThat(db.queryForObject("SELECT status FROM transfer_task WHERE id=?", String.class, accepted.taskRecordId)).isEqualTo("RUNNING");
            assertThat(db.queryForObject("SELECT count(*) FROM transfer_notification WHERE task_id=?", Integer.class, accepted.taskRecordId)).isZero();
        } finally {
            db.execute("DROP TRIGGER fail_notification_insert ON transfer_notification");
            db.execute("DROP FUNCTION fail_notification()");
        }
        db.update("UPDATE transfer_task SET claimed_at=now()-interval '6 minutes' WHERE id=?", accepted.taskRecordId);
        worker.poll();
        await(() -> "SUCCEEDED".equals(db.queryForObject("SELECT status FROM transfer_task WHERE id=?", String.class, accepted.taskRecordId)));
        assertThat(db.queryForObject("SELECT count(*) FROM transfer_notification WHERE task_id=?", Integer.class, accepted.taskRecordId)).isEqualTo(1);
    }

    private static void await(java.util.function.BooleanSupplier done) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            if (done.getAsBoolean()) return;
            Thread.sleep(50);
        }
        throw new AssertionError("Timed out waiting for task state");
    }

    private String notificationStatus(String id) {
        try { return db.queryForObject("SELECT status FROM transfer_notification WHERE task_id=?", String.class, id); }
        catch (org.springframework.dao.EmptyResultDataAccessException e) { return null; }
    }

    @AfterAll static void cleanup() throws Exception {
        if (receiver != null) receiver.stop(0);
        if (!SCHEMA.matches("task_it_[a-f0-9]{32}")) throw new IllegalStateException("Unsafe schema cleanup");
        try (var connection = DriverManager.getConnection(URL, USER, PASSWORD); var statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE");
        }
    }
}
