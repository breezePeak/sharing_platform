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

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
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
    private static final ConcurrentMap<String, AtomicInteger> receiverEffectsByTask = new ConcurrentHashMap<>();
    private static final Set<String> processedNotifications = ConcurrentHashMap.newKeySet();
    private static final AtomicInteger blockedStarted = new AtomicInteger();
    private static volatile CountDownLatch blockRelease = new CountDownLatch(0);
    private static final List<String> payloads = new CopyOnWriteArrayList<>();
    private static volatile boolean wrongAck;
    private static final Set<String> ackFaults = ConcurrentHashMap.newKeySet();
    private static volatile CountDownLatch releaseAck = new CountDownLatch(0);
    private static HttpServer receiver;

    private static synchronized int receiverPort() {
        if (receiver == null) try {
            receiver = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            receiver.setExecutor(Executors.newCachedThreadPool(r -> { Thread t = new Thread(r, "ack-fixture"); t.setDaemon(true); return t; }));
            receiver.createContext("/notify", exchange -> {
                String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                payloads.add(body);
                var node = new ObjectMapper().readTree(body);
                String taskName = node.path("taskId").asText();
                int received = receivedByTask.computeIfAbsent(node.path("taskRecordId").asText(), ignored -> new AtomicInteger()).incrementAndGet();
                if (processedNotifications.add(node.path("notificationId").asText()))
                    receiverEffectsByTask.computeIfAbsent(node.path("taskRecordId").asText(), ignored -> new AtomicInteger()).incrementAndGet();
                if (taskName.equals("ack-loss") && received == 1) { exchange.close(); return; }
                String id = wrongAck || node.path("taskId").asText().equals("attention") ? "wrong" : node.path("notificationId").asText();
                String ack = "{\"accepted\":true,\"notificationId\":\"" + id
                        + "\",\"taskRecordId\":\"" + node.path("taskRecordId").asText() + "\"}";
                byte[] bytes = (taskName.equals("ack-large") && ackFaults.contains(taskName) ? "x".repeat(5000) : ack).getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                if (ackFaults.contains(taskName) && (taskName.equals("ack-hang") || taskName.equals("ack-slow"))) {
                    exchange.sendResponseHeaders(200, 0);
                    try (var out = exchange.getResponseBody()) {
                        if (taskName.equals("ack-slow")) {
                            long until = System.nanoTime() + Duration.ofSeconds(15).toNanos();
                            while (releaseAck.getCount() > 0 && System.nanoTime() < until) {
                                out.write(bytes, 0, 1); out.flush(); Thread.sleep(200);
                            }
                        } else releaseAck.await(15, TimeUnit.SECONDS);
                        out.write(bytes);
                    } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                } else {
                    exchange.sendResponseHeaders(200, bytes.length);
                    try (var out = exchange.getResponseBody()) { out.write(bytes); }
                }
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
        p.add("platform.transfers.push-enabled", () -> false);
        p.add("platform.endpoints.local-roots.local", () -> System.getProperty("java.io.tmpdir"));
    }

    @TestConfiguration static class ProcessorConfig {
        @Bean @org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(
                name = "platform.transfers.push-enabled", havingValue = "false")
        TaskProcessor testProcessor() {
            return new TaskProcessor() {
                public String direction() { return "PUSH"; }
                public Result process(Task task) {
                    executionsByTask.computeIfAbsent(task.taskId, ignored -> new AtomicInteger()).incrementAndGet();
                    if (task.taskId.startsWith("block-") || task.taskId.equals("stale") || task.taskId.equals("late-claim")) {
                        blockedStarted.incrementAndGet();
                        try { blockRelease.await(15, TimeUnit.SECONDS); }
                        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
                    }
                    if ((task.taskId.equals("precheck") || task.taskId.equals("ack-rollback")) && task.phase.equals("ACCEPTED"))
                        return new Result("WAITING_FEEDBACK", "PRECHECK", null, null);
                    return new Result("SUCCEEDED", "COMPLETE", null, null);
                }
            };
        }
    }

    @Autowired TaskService service;
    @Autowired TaskWorker worker;
    @Autowired NotificationDispatcher dispatcher;
    @Autowired com.siwei.web.TransferController controller;
    @Autowired TransferSettings transferSettings;
    @Autowired JdbcTemplate db;
    @Autowired MockMvc mvc;
    @org.springframework.boot.web.server.LocalServerPort int httpPort;

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

    private static String xmlRequest(String taskId, String productId) {
        return "<taskNotice><taskId>" + taskId + "</taskId><type>weather</type><sourceRoot>local</sourceRoot>"
                + "<targetPath>output</targetPath><fileCount>1</fileCount><ProductList><product>"
                + "<productId>" + productId + "</productId><productPath>p</productPath><productName></productName>"
                + "</product></ProductList></taskNotice>";
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
            await(() -> db.queryForObject("SELECT count(*) FROM transfer_notification WHERE task_id=?", Integer.class,
                    ids.iterator().next()) == 1);
            String id = ids.iterator().next();
            await(() -> "ACKED".equals(notificationStatus(id)));
        } finally { pool.shutdownNow(); }
    }

    @Test void replayConflictsReturn409WithoutChangingStoredTask() throws Exception {
        String body = request("four-conflicts");
        var accepted = service.accept("test", service.parseJson(body), body, "JSON");
        await(() -> "ACKED".equals(notificationStatus(accepted.taskRecordId)));
        String canonical = db.queryForObject("SELECT canonical_request FROM transfer_task WHERE id=?", String.class, accepted.taskRecordId);
        String originalStatus = db.queryForObject("SELECT status FROM transfer_task WHERE id=?", String.class, accepted.taskRecordId);
        String[] changed = {
                body.replace("\"direction\":\"PUSH\"", "\"direction\":\"PULL\""),
                body.replace(ENDPOINT, UUID.randomUUID().toString()),
                body.replace("\"productId\":\"p\"", "\"productId\":\"q\""),
                body.replace("\"targetPath\":\"output\"", "\"targetPath\":\"elsewhere\"")
        };
        for (String replay : changed) {
            mvc.perform(post("/api/transfers/tasks").header("Authorization", "Bearer test-secret")
                    .contentType("application/json").content(replay)).andExpect(status().isConflict());
        }
        assertThat(db.queryForObject("SELECT count(*) FROM transfer_task WHERE external_task_id='four-conflicts'", Integer.class)).isEqualTo(1);
        assertThat(db.queryForObject("SELECT canonical_request FROM transfer_task WHERE id=?", String.class, accepted.taskRecordId))
                .isEqualTo(canonical);
        assertThat(db.queryForObject("SELECT count(*) FROM transfer_product WHERE task_id=?", Integer.class, accepted.taskRecordId)).isEqualTo(1);
        assertThat(db.queryForObject("SELECT status FROM transfer_task WHERE id=?", String.class, accepted.taskRecordId)).isEqualTo(originalStatus);
        assertThat(executionsByTask.get("four-conflicts").get()).isEqualTo(1);
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

    @Test void lostAckRetriesSameNotificationWithoutRerunningTask() throws Exception {
        payloads.clear();
        String body = request("ack-loss");
        var accepted = service.accept("test", service.parseJson(body), body, "JSON");
        await(() -> receivedByTask.getOrDefault(accepted.taskRecordId, new AtomicInteger()).get() >= 1);
        await(() -> "RETRY_WAIT".equals(notificationStatus(accepted.taskRecordId)));
        db.update("UPDATE transfer_notification SET next_attempt_at=now() WHERE task_id=?", accepted.taskRecordId);
        await(() -> "ACKED".equals(notificationStatus(accepted.taskRecordId)));
        assertThat(executionsByTask.get("ack-loss").get()).isEqualTo(1);
        assertThat(receiverEffectsByTask.get(accepted.taskRecordId).get()).isEqualTo(1);
        List<String> sent = new ArrayList<>();
        for (String payload : payloads) if (payload.contains(accepted.taskRecordId)) sent.add(payload);
        assertThat(sent).hasSizeGreaterThanOrEqualTo(2).containsOnly(sent.get(0));
        assertThat(db.queryForObject("SELECT count(DISTINCT id) FROM transfer_notification WHERE task_id=?", Integer.class,
                accepted.taskRecordId)).isEqualTo(1);
    }

    @Test void oversizedAckIsRejectedDuringReadAndNextDeliveryProgresses() throws Exception {
        assertAckFaultRetries("ack-large");
    }

    @Test void hangingAndSlowAckBodiesRespectTotalDeadline() throws Exception {
        assertAckFaultRetries("ack-hang");
        assertAckFaultRetries("ack-slow");
    }

    private void assertAckFaultRetries(String taskName) throws Exception {
        ackFaults.add(taskName);
        releaseAck = new CountDownLatch(1);
        String body = request(taskName);
        var accepted = service.accept("test", service.parseJson(body), body, "JSON");
        try {
            await(() -> "RETRY_WAIT".equals(notificationStatus(accepted.taskRecordId)));
            assertThat(executionsByTask.get(taskName).get()).isEqualTo(1);
            assertThat(db.queryForObject("SELECT attempts FROM transfer_notification WHERE task_id=?", Integer.class,
                    accepted.taskRecordId)).isEqualTo(1);
            db.update("UPDATE transfer_notification SET next_attempt_at=now()+interval '1 minute' WHERE task_id=?", accepted.taskRecordId);
            String following = request("after-" + taskName);
            var next = service.accept("test", service.parseJson(following), following, "JSON");
            await(() -> "ACKED".equals(notificationStatus(next.taskRecordId)));
            ackFaults.remove(taskName);
            releaseAck.countDown();
            db.update("UPDATE transfer_notification SET next_attempt_at=now() WHERE task_id=?", accepted.taskRecordId);
            await(() -> "ACKED".equals(notificationStatus(accepted.taskRecordId)));
            assertThat(executionsByTask.get(taskName).get()).isEqualTo(1);
        } finally { ackFaults.remove(taskName); releaseAck.countDown(); }
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

    @Test void requestLimitPrecedesBodyBindingForLengthAndChunked() throws Exception {
        String huge = request("too-large") + " ".repeat(32769);
        assertThat(realPost(request("normal-http"), false, true)).isEqualTo(202);
        assertThat(realPost(huge, false, true)).isEqualTo(413);
        assertThat(realPost(huge, true, true)).isEqualTo(413);
        assertThat(realPost(huge, true, false)).isEqualTo(401);
        var chunked = new org.springframework.mock.web.MockHttpServletRequest() {
            @Override public long getContentLengthLong() { return -1; }
        };
        chunked.setContent(huge.getBytes(StandardCharsets.UTF_8));
        chunked.addHeader("Transfer-Encoding", "chunked");
        assertThatThrownBy(() -> controller.create("Bearer test-secret", chunked))
                .isInstanceOf(org.springframework.web.server.ResponseStatusException.class)
                .satisfies(error -> assertThat(((org.springframework.web.server.ResponseStatusException) error).getStatus())
                        .isEqualTo(org.springframework.http.HttpStatus.PAYLOAD_TOO_LARGE));
        mvc.perform(post("/api/transfers/tasks").contentType("application/json").content(huge))
                .andExpect(status().isUnauthorized());
        assertThat(db.queryForObject("SELECT count(*) FROM transfer_task WHERE external_task_id='too-large'", Integer.class)).isZero();
    }

    private int realPost(String body, boolean chunked, boolean authenticated) throws Exception {
        return realPost(body, chunked, authenticated, false);
    }
    private int realPost(String body, boolean chunked, boolean authenticated, boolean xml) throws Exception {
        var connection = (java.net.HttpURLConnection) new java.net.URL("http://127.0.0.1:" + httpPort
                + (xml ? "/api/legacy/task-notices" : "/api/transfers/tasks")).openConnection();
        connection.setRequestMethod("POST"); connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", xml ? "application/xml" : "application/json");
        if (authenticated) connection.setRequestProperty("Authorization", "Bearer test-secret");
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        if (chunked) connection.setChunkedStreamingMode(1024); else connection.setFixedLengthStreamingMode(bytes.length);
        try {
            try (var out = connection.getOutputStream()) { out.write(bytes); }
            return connection.getResponseCode();
        } finally { connection.disconnect(); }
    }

    @Test void equivalentXmlAndJsonReuseOneTaskAndXxeDoesNotReachDatabase() {
        String productId = "12345678901234567890123456789012";
        String body = request("cross-format").replace("\"sourceRoot\"", "\"sourceCode\":\"weather\",\"sourceRoot\"")
                .replace("\"productId\":\"p\"", "\"productId\":\"" + productId + "\"");
        var first = service.accept("test", service.parseJson(body), body, "JSON");
        String xml = "<taskNotice><taskId>cross-format</taskId><type>weather</type><sourceRoot>local</sourceRoot>"
                + "<targetPath>output</targetPath><fileCount>1</fileCount><ProductList><product>"
                + "<productId>" + productId + "</productId><productPath>p</productPath><productName></productName>"
                + "</product></ProductList></taskNotice>";
        var repeat = service.accept("test", service.parseXml(xml), xml, "XML");
        assertThat(repeat.reused).isTrue(); assertThat(repeat.taskRecordId).isEqualTo(first.taskRecordId);
        assertThatThrownBy(() -> service.parseXml("<!DOCTYPE taskNotice [<!ENTITY xxe SYSTEM 'file:///etc/passwd'>]>" + xml))
                .isInstanceOf(org.springframework.web.server.ResponseStatusException.class);
        assertThat(db.queryForObject("SELECT count(*) FROM transfer_task WHERE external_task_id='cross-format'", Integer.class)).isEqualTo(1);
    }

    @Test void xmlProductIdsMustBeExactly32AndReverseCrossFormatReuses() {
        String id32 = "12345678901234567890123456789012";
        assertThatThrownBy(() -> service.parseXml(xmlRequest("xml-31", id32.substring(1))))
                .isInstanceOf(org.springframework.web.server.ResponseStatusException.class);
        assertThatThrownBy(() -> service.parseXml(xmlRequest("xml-33", id32 + "3")))
                .isInstanceOf(org.springframework.web.server.ResponseStatusException.class);
        String xml = xmlRequest("xml-first", id32);
        var first = service.accept("test", service.parseXml(xml), xml, "XML");
        String json = request("xml-first").replace("\"sourceRoot\"", "\"sourceCode\":\"weather\",\"sourceRoot\"")
                .replace("\"productId\":\"p\"", "\"productId\":\"" + id32 + "\"");
        assertThat(service.accept("test", service.parseJson(json), json, "JSON").taskRecordId).isEqualTo(first.taskRecordId);
        String duplicate = xml.replace("<fileCount>1</fileCount>", "<fileCount>2</fileCount>")
                .replace("</ProductList>", "<product><productId>" + id32 + "</productId><productPath>other</productPath></product></ProductList>");
        assertThatThrownBy(() -> service.accept("test", service.parseXml(duplicate), duplicate, "XML"))
                .isInstanceOf(org.springframework.web.server.ResponseStatusException.class);
        assertThat(db.queryForObject("SELECT count(*) FROM transfer_product WHERE task_id=?", Integer.class, first.taskRecordId)).isEqualTo(1);
    }

    @Test void xmlDepthAndNodeLimitsRejectSmallHttpBodies() throws Exception {
        String id32 = "12345678901234567890123456789012";
        String xml = xmlRequest("xml-complex", id32);
        String deep = xml.replace(id32, "<x>".repeat(17) + id32 + "</x>".repeat(17));
        String many = xml.replace("</taskNotice>", "<!---->".repeat(4100) + "</taskNotice>");
        assertThat(deep.getBytes(StandardCharsets.UTF_8).length).isLessThan(32768);
        assertThat(many.getBytes(StandardCharsets.UTF_8).length).isLessThan(32768);
        assertThat(realPost(deep, false, true, true)).isEqualTo(400);
        assertThat(realPost(many, true, true, true)).isEqualTo(400);
        assertThat(db.queryForObject("SELECT count(*) FROM transfer_task WHERE external_task_id='xml-complex'", Integer.class)).isZero();
    }

    @Test void xmlReplayKeepsOriginalRouteWhenConfigurationChanges() {
        String xml = xmlRequest("route-stable", "12345678901234567890123456789012");
        var first = service.accept("test", service.parseXml(xml), xml, "XML");
        TransferSettings.Caller caller = transferSettings.getCallers().get("test");
        try {
            caller.setXmlRoutes(Map.of("weather", OUTSIDER_ENDPOINT));
            var replay = service.accept("test", service.parseXml(xml), xml, "XML");
            assertThat(replay.reused).isTrue();
            assertThat(replay.taskRecordId).isEqualTo(first.taskRecordId);
        } finally { caller.setXmlRoutes(Map.of("weather", ENDPOINT)); }
    }

    @Test void precheckAckResumesTheSameTaskAndKeepsBothNotifications() throws Exception {
        String body = request("precheck");
        var accepted = service.accept("test", service.parseJson(body), body, "JSON");
        await(() -> db.queryForObject("SELECT count(*) FROM transfer_notification WHERE task_id=? AND status='ACKED'", Integer.class, accepted.taskRecordId) == 2);
        assertThat(db.queryForObject("SELECT status FROM transfer_task WHERE id=?", String.class, accepted.taskRecordId)).isEqualTo("SUCCEEDED");
        assertThat(db.queryForObject("SELECT count(DISTINCT stage) FROM transfer_notification WHERE task_id=?", Integer.class, accepted.taskRecordId)).isEqualTo(2);
    }

    @Test @org.junit.jupiter.api.extension.ExtendWith(OutputCaptureExtension.class)
    void ackAndPrecheckResumeRollBackTogether(CapturedOutput output) throws Exception {
        db.execute("CREATE FUNCTION fail_resume() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.phase='PRECHECK_ACKED' AND OLD.external_task_id='ack-rollback' THEN RAISE EXCEPTION 'forced'; END IF; RETURN NEW; END $$");
        db.execute("CREATE TRIGGER fail_resume_update BEFORE UPDATE ON transfer_task FOR EACH ROW EXECUTE FUNCTION fail_resume()");
        String body = request("ack-rollback");
        var accepted = service.accept("test", service.parseJson(body), body, "JSON");
        try {
            await(() -> output.toString().contains("notification ACK persistence failed id="));
            assertThat(notificationStatus(accepted.taskRecordId)).isEqualTo("SENDING");
            assertThat(db.queryForObject("SELECT status FROM transfer_task WHERE id=?", String.class, accepted.taskRecordId))
                    .isEqualTo("WAITING_FEEDBACK");
            assertThat(db.queryForObject("SELECT ack FROM transfer_notification WHERE task_id=?", String.class,
                    accepted.taskRecordId)).isNull();
        } finally {
            db.execute("DROP TRIGGER fail_resume_update ON transfer_task");
            db.execute("DROP FUNCTION fail_resume()");
        }
        db.update("UPDATE transfer_notification SET claimed_at=now()-interval '2 minutes' WHERE task_id=?", accepted.taskRecordId);
        dispatcher.poll();
        await(() -> db.queryForObject("SELECT count(*) FROM transfer_notification WHERE task_id=? AND status='ACKED'",
                Integer.class, accepted.taskRecordId) == 2);
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

    @Test void jpaReadsPreexistingV3RowsWithoutMigrationChanges() {
        String id = UUID.randomUUID().toString();
        String revision = db.queryForObject("SELECT id FROM endpoint_revision WHERE endpoint_id=? AND revision=1", String.class, ENDPOINT);
        db.update("INSERT INTO transfer_task (id,caller_id,external_task_id,entry_type,direction,endpoint_id,endpoint_revision_id,notification_url,canonical_request,raw_request,status,phase,next_run_at,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,'SUCCEEDED','COMPLETE',now(),now(),now())",
                id, "test", "old-v3", "JSON", "PUSH", ENDPOINT, revision, "http://127.0.0.1/ack", "{}", "{}" );
        db.update("INSERT INTO transfer_product (task_id,position,product_id,product_path) VALUES (?,?,?,?)", id, 0, "existing", "p");
        Task task = service.get("test", id);
        assertThat(task.taskId).isEqualTo("old-v3");
        assertThat(task.executionStatus).isEqualTo("SUCCEEDED");
        assertThat(task.products).hasSize(1);
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

    @Test void expiredLocalClaimDoesNotStarveOtherQueuedTask() throws Exception {
        blockedStarted.set(0); blockRelease = new CountDownLatch(1);
        String blocked = request("block-stale");
        var first = service.accept("test", service.parseJson(blocked), blocked, "JSON");
        try {
            await(() -> blockedStarted.get() == 1);
            db.update("UPDATE transfer_task SET claimed_at=now()-interval '6 minutes' WHERE id=?", first.taskRecordId);
            String ready = request("available-slot");
            var second = service.accept("test", service.parseJson(ready), ready, "JSON");
            worker.poll();
            await(() -> "SUCCEEDED".equals(db.queryForObject("SELECT status FROM transfer_task WHERE id=?", String.class, second.taskRecordId)));
            assertThat(blockedStarted.get()).isEqualTo(1);
            assertThat(db.queryForObject("SELECT attempts FROM transfer_task WHERE id=?", Integer.class, first.taskRecordId)).isEqualTo(1);
        } finally { blockRelease.countDown(); }
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
            assertThat(blockedStarted.get()).isEqualTo(1);
            assertThat(db.queryForObject("SELECT attempts FROM transfer_task WHERE id=?", Integer.class, accepted.taskRecordId)).isEqualTo(1);
        } finally { blockRelease.countDown(); }
        await(() -> "SUCCEEDED".equals(db.queryForObject("SELECT status FROM transfer_task WHERE id=?", String.class, accepted.taskRecordId)));
        assertThat(db.queryForObject("SELECT count(*) FROM transfer_notification WHERE task_id=?", Integer.class, accepted.taskRecordId)).isEqualTo(1);
    }

    @Test void lateOriginalClaimCannotCommitAfterExternalReclaim() throws Exception {
        blockedStarted.set(0); blockRelease = new CountDownLatch(1);
        String body = request("late-claim");
        var accepted = service.accept("test", service.parseJson(body), body, "JSON");
        try {
            await(() -> blockedStarted.get() == 1);
            db.update("UPDATE transfer_task SET claim_id=?,claimed_at=now() WHERE id=?", UUID.randomUUID().toString(), accepted.taskRecordId);
        } finally { blockRelease.countDown(); }
        @SuppressWarnings("unchecked") ConcurrentMap<String, String> active =
                (ConcurrentMap<String, String>) org.springframework.test.util.ReflectionTestUtils.getField(worker, "active");
        await(() -> !active.containsKey(accepted.taskRecordId));
        assertThat(db.queryForObject("SELECT status FROM transfer_task WHERE id=?", String.class, accepted.taskRecordId)).isEqualTo("RUNNING");
        assertThat(db.queryForObject("SELECT count(*) FROM transfer_notification WHERE task_id=?", Integer.class,
                accepted.taskRecordId)).isZero();
        db.update("UPDATE transfer_task SET claimed_at=now()-interval '6 minutes' WHERE id=?", accepted.taskRecordId);
        worker.poll();
        await(() -> "SUCCEEDED".equals(db.queryForObject("SELECT status FROM transfer_task WHERE id=?", String.class, accepted.taskRecordId)));
        assertThat(db.queryForObject("SELECT count(*) FROM transfer_notification WHERE task_id=?", Integer.class,
                accepted.taskRecordId)).isEqualTo(1);
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
