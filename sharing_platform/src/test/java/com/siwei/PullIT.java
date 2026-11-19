package com.siwei;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.siwei.endpoint.*;
import com.siwei.protocol.*;
import com.siwei.transfer.*;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.server.ResponseStatusException;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@ActiveProfiles("test")
@AutoConfigureMockMvc
@DirtiesContext
class PullIT {
    @org.springframework.boot.test.context.TestConfiguration
    static class TestTime {
        @org.springframework.context.annotation.Bean @org.springframework.context.annotation.Primary
        MutableClock testClock() { return new MutableClock(); }
    }
    static class MutableClock extends java.time.Clock {
        final AtomicReference<Instant> time = new AtomicReference<>();
        @Override public ZoneId getZone() { return ZoneId.of("UTC"); }
        @Override public java.time.Clock withZone(ZoneId zone) { return java.time.Clock.fixed(instant(), zone); }
        @Override public Instant instant() { Instant value = time.get(); return value == null ? Instant.now() : value; }
        void advance(int seconds) { time.updateAndGet(value -> value.plusSeconds(seconds)); }
    }
    private static final String URL = TestDatabase.requireUrl(System.getenv());
    private static final String USER = TestDatabase.require(System.getenv(), "TEST_DB_USERNAME");
    private static final String PASSWORD = TestDatabase.require(System.getenv(), "TEST_DB_PASSWORD");
    private static final String SCHEMA = "pull_it_" + UUID.randomUUID().toString().replace("-", "");
    private static final Path DELIVERY;
    private static final List<String> payloads = new CopyOnWriteArrayList<>();
    private static final Set<String> lostAcks = ConcurrentHashMap.newKeySet();
    private static HttpServer receiver;
    static {
        try { DELIVERY = Files.createTempDirectory("pull-it-delivery-"); }
        catch (java.io.IOException error) { throw new ExceptionInInitializerError(error); }
    }
    private static synchronized int receiverPort() {
        if (receiver == null) try {
            receiver = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            receiver.createContext("/notify", exchange -> {
                String payload = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8); payloads.add(payload);
                JsonNode node = new ObjectMapper().readTree(payload);
                if (lostAcks.contains(node.path("taskId").asText())) { exchange.close(); return; }
                byte[] bytes = ("{\"accepted\":true,\"notificationId\":\"" + node.path("notificationId").asText()
                        + "\",\"taskRecordId\":\"" + node.path("taskRecordId").asText() + "\"}").getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, bytes.length);
                try (var out = exchange.getResponseBody()) { out.write(bytes); }
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
        p.add("platform.endpoints.local-roots.delivery", () -> DELIVERY.toString());
        p.add("platform.transfers.callers.pull.token", () -> "pull-test-token");
        p.add("platform.transfers.callers.pull.notification-url", () -> "http://127.0.0.1:" + receiverPort() + "/notify");
        p.add("platform.transfers.callers.other.token", () -> "other-test-token");
        p.add("platform.transfers.callers.other.notification-url", () -> "http://127.0.0.1:" + receiverPort() + "/notify");
        p.add("platform.pulls.listeners.workers", () -> "2");
    }
    @Autowired EndpointService endpoints;
    @Autowired EndpointRevisionRepository revisions;
    @Autowired EndpointSettings endpointSettings;
    @org.springframework.boot.test.mock.mockito.SpyBean RemoteFileClientFactory clients;
    @Autowired PullSettings pullSettings;
    @Autowired TransferSettings transferSettings;
    @Autowired PullProcessor pulls;
    @Autowired PullListenerService listeners;
    @Autowired PullListenerScanner scanner;
    @Autowired PullStore store;
    @Autowired TransferJpa taskDb;
    @Autowired TaskService tasks;
    @Autowired TaskWorker worker;
    @Autowired NotificationDispatcher notifications;
    @org.springframework.boot.test.mock.mockito.MockBean org.springframework.scheduling.TaskScheduler scheduler;
    @Autowired org.springframework.transaction.PlatformTransactionManager manager;
    @Autowired MutableClock clock;
    @Autowired ObjectMapper json;
    @Autowired JdbcTemplate db;
    @Autowired MockMvc mvc;
    @TempDir Path remoteRoot;

    @AfterEach void stopListeners() {
        db.update("UPDATE pull_listener SET enabled=false,manual_pending=false");
        clock.time.set(null);
    }

    @Test void listenersKeepProtocolTargetsIndependentAndCoalesceDueScans() throws Exception {
        clock.time.set(Instant.now());
        Path ftpRoot = remoteRoot.resolve("ftp");
        Files.createDirectories(ftpRoot.resolve("north")); Files.createDirectories(ftpRoot.resolve("south"));
        Files.writeString(ftpRoot.resolve("north/a.bin"), "north");
        Files.writeString(ftpRoot.resolve("south/b.bin"), "south");
        try (ProtocolServers ftp = new ProtocolServers(ftpRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL);
             ProtocolServers sftp = new ProtocolServers(remoteRoot.resolve("sftp"), EndpointRevision.Protocol.SFTP, ProtocolServers.Mode.NORMAL);
             StreamingFtpServer broken = new StreamingFtpServer(StreamingFtpServer.Fault.WELCOME)) {
            String northSource = source(ftp, EndpointRevision.Protocol.FTP, "FILE", "north");
            String ftpEndpoint = pullSettings.getSources().get(northSource).getEndpointId();
            String southSource = sourceOn(ftpEndpoint, "FILE", "south");
            String sshSource = source(sftp, EndpointRevision.Protocol.SFTP, "FILE", "sample.bin");
            EndpointForm unreachable = ftp.form(EndpointRevision.Protocol.FTP);
            unreachable.port = broken.port(); unreachable.timeoutSeconds = 1; unreachable.localTargetRef = "delivery";
            String brokenEndpoint = endpoints.save(null, unreachable);
            transferSettings.getCallers().get("pull").getEndpointIds().add(brokenEndpoint);
            String brokenSource = sourceOn(brokenEndpoint, "FILE", "incoming");
            for (String source : List.of(northSource, southSource, brokenSource)) configured(source).setScanDepth(1);
            for (String source : List.of(northSource, southSource, sshSource)) configured(source).setMinimumObservationSeconds(0);
            String north = listener(northSource, 60, "IMPORT_EXISTING");
            String south = listener(southSource, 75, "IMPORT_EXISTING");
            String ssh = listener(sshSource, 120, "IMPORT_EXISTING");
            String down = listener(brokenSource, 60, "IMPORT_EXISTING");
            CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
            doAnswer(call -> {
                RemoteFileClient real = (RemoteFileClient) call.callRealMethod();
                if (!ftpEndpoint.equals(call.<EndpointRevision>getArgument(0).endpointId)) return real;
                RemoteFileClient remote = mock(RemoteFileClient.class, org.mockito.AdditionalAnswers.delegatesTo(real));
                doAnswer(list -> {
                    entered.countDown();
                    if (!release.await(15, TimeUnit.SECONDS)) throw new java.io.IOException("TEST_SCAN_TIMEOUT");
                    return real.list("/north");
                }).when(remote).list("/north");
                return remote;
            }).when(clients).connect(any());
            try {
                listeners.manual(north); // Ensure the first FTP target holds its endpoint's scan slot.
                clock.advance(1);
                db.update("UPDATE pull_listener SET next_run_at=? WHERE id=?", java.sql.Timestamp.from(clock.instant().minusSeconds(2)), north);
                db.update("UPDATE pull_listener SET next_run_at=? WHERE id=?", java.sql.Timestamp.from(clock.instant().minusSeconds(1)), ssh);
                pollListeners();
                assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                await(() -> "SUCCEEDED".equals(latest(ssh).get("status")));
                assertThat(runCount(down)).isZero();
                await(() -> {
                    pollListeners();
                    return "FAILED".equals(latest(down).get("status"));
                });
                assertThat(latest(down).get("error_type")).isEqualTo("SCAN_IO");
                assertThat(runCount(south)).isZero();
                clock.advance(61);
                pollListeners();
                assertThat(runCount(north)).isEqualTo(1);
                assertThat(latest(north).get("coalesced_reason")).isEqualTo("PERIOD_COALESCED");
                assertThat(latest(ssh).get("created_count")).isEqualTo(1);
            } finally { release.countDown(); }
            await(() -> "SUCCEEDED".equals(latest(north).get("status")));
            await(() -> {
                pollListeners();
                return "SUCCEEDED".equals(latest(south).get("status"));
            });
            assertThat(latest(south).get("created_count")).isEqualTo(1);
            assertThat(listenerTasks(south)).hasSize(1);
            assertThatThrownBy(() -> listenerRequest(north, configured(sshSource).getEndpointId(), null))
                    .hasMessageContaining("LISTENER_ENDPOINT_MISMATCH");
            String task = listenerTasks(ssh).get(0);
            runOnce(task, pulls);
            assertThat(state(task)).isEqualTo("WAITING_ACCEPTANCE");
        }
    }

    @Test void stableObservationsUseTheClockAndOnlyCompleteInventoriesChangeCandidates() throws Exception {
        clock.time.set(Instant.now());
        Path north = Files.createDirectories(remoteRoot.resolve("incoming/north"));
        Path south = Files.createDirectories(remoteRoot.resolve("incoming/south"));
        Files.writeString(north.resolve("a.bin"), "first");
        Files.writeString(south.resolve("b.bin"), "second");
        try (ProtocolServers ftp = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            String source = source(ftp, EndpointRevision.Protocol.FTP, "FILE", "incoming");
            var config = configured(source);
            config.setScanDepth(2); config.setStableObservations(3); config.setObservationSeconds(30);
            String listener = listener(source, 60, "IMPORT_EXISTING");
            manualScan(listener);
            assertThat(stability(source, "/incoming/north/a.bin")).isEqualTo(1);
            clock.advance(30);
            Files.delete(north.resolve("a.bin"));
            AtomicBoolean partial = new AtomicBoolean(true);
            doAnswer(call -> {
                RemoteFileClient real = (RemoteFileClient) call.callRealMethod();
                RemoteFileClient remote = mock(RemoteFileClient.class, org.mockito.AdditionalAnswers.delegatesTo(real));
                doAnswer(list -> {
                    if (partial.get()) throw new java.io.IOException("TEST_PARTIAL_SCAN");
                    return real.list("/incoming/south");
                }).when(remote).list("/incoming/south");
                return remote;
            }).when(clients).connect(any());
            assertThat(manualScan(listener).get("status")).isEqualTo("FAILED");
            assertThat(stability(source, "/incoming/north/a.bin")).isEqualTo(1);
            assertThat(stability(source, "/incoming/south/b.bin")).isEqualTo(1);
            partial.set(false);
            manualScan(listener);
            assertThat(stability(source, "/incoming/north/a.bin")).isZero();
            assertThat(stability(source, "/incoming/south/b.bin")).isEqualTo(2);
            Files.writeString(north.resolve("a.bin"), "new content size");
            clock.advance(30); manualScan(listener);
            assertThat(stability(source, "/incoming/north/a.bin")).isEqualTo(1);
            assertThat(stability(source, "/incoming/south/b.bin")).isEqualTo(3);
            assertThat(listenerTasks(listener)).isEmpty();
            clock.advance(60); manualScan(listener);
            assertThat(listenerTasks(listener)).hasSize(1); // Only south has reached 120 seconds.
            assertThat(listenerTasks(listener).get(0)).isNotBlank();
            clock.advance(60); manualScan(listener);
            assertThat(latest(listener).get("created_count")).isEqualTo(1);
        }
    }

    @Test void baselineSurvivesAnExpiredClaimAndNamePeriodAndScopeChanges() throws Exception {
        clock.time.set(Instant.now());
        Path area = Files.createDirectories(remoteRoot.resolve("incoming"));
        Files.createDirectories(area.resolve("archive"));
        Files.writeString(area.resolve("a.bin"), "old");
        try (ProtocolServers ftp = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            String source = source(ftp, EndpointRevision.Protocol.FTP, "FILE", "incoming");
            configured(source).setScanDepth(1);
            configured(source).setMinimumObservationSeconds(0);
            String listener = listener(source, 60, "BASELINE_ONLY");
            var config = listeners.form(listener);
            String endpoint = (String) config.get("endpoint_id");
            AtomicBoolean interruptedInventory = new AtomicBoolean(true);
            doAnswer(call -> {
                RemoteFileClient real = (RemoteFileClient) call.callRealMethod();
                RemoteFileClient remote = mock(RemoteFileClient.class, org.mockito.AdditionalAnswers.delegatesTo(real));
                doAnswer(list -> {
                    if (interruptedInventory.get()) throw new java.io.IOException("TEST_BASELINE_INTERRUPTED");
                    return real.list("/incoming");
                }).when(remote).list("/incoming");
                return remote;
            }).when(clients).connect(any());
            assertThat(manualScan(listener).get("status")).isEqualTo("FAILED");
            assertThat(listeners.form(listener).get("initialized_at")).isNull();
            assertThat(count("pull_listener_baseline", "listener_id", listener)).isZero();
            interruptedInventory.set(false);
            String abandoned = UUID.randomUUID().toString();
            var expired = java.sql.Timestamp.from(clock.instant().minusSeconds(180));
            db.update("INSERT INTO pull_listener_run(id,listener_id,revision,endpoint_revision_id,trigger_type,status,started_at,heartbeat_at) "
                    + "VALUES (?,?,1,?,'SCHEDULE','SCANNING',?,?)", abandoned, listener, endpoints.current(endpoint).id, expired, expired);
            assertThat(config.get("initialized_at")).isNull();
            clock.advance(1);
            PullListenerScanner restarted = new PullListenerScanner(store, listeners, pullSettings, pulls, tasks, clients, manager, clock, true, 2, 1);
            try {
                restarted.poll();
                await(() -> "BASELINED".equals(latest(listener).get("status")));
            } finally { restarted.close(); }
            assertThat(db.queryForObject("SELECT status FROM pull_listener_run WHERE id=?", String.class, abandoned)).isEqualTo("INTERRUPTED");
            assertThat(count("pull_listener_baseline", "listener_id", listener)).isEqualTo(1);
            assertThat(listenerTasks(listener)).isEmpty();
            var initialized = listeners.form(listener).get("initialized_at");
            assertThat(initialized).isNotNull();
            clock.advance(1);
            listeners.save(listener, 1, "renamed", source, "incoming", 120, "BASELINE_ONLY", false);
            listeners.toggle(listener, 2);
            assertThat(manualScan(listener).get("created_count")).isEqualTo(0);
            assertThat(listeners.form(listener).get("enabled")).isEqualTo(false);
            assertThat(listeners.form(listener).get("revision")).isEqualTo(1);
            assertThat(listeners.form(listener).get("initialized_at")).isEqualTo(initialized);
            String baselineApi = listenerRequest(listener, endpoint, "incoming/a.bin");
            runOnce(baselineApi, pulls);
            assertThat(state(baselineApi)).isEqualTo("NO_NEW_DATA");
            String separateSource = sourceOn(endpoint, "FILE", "incoming/a.bin");
            configured(separateSource).setRecipientId("other");
            configured(separateSource).setMinimumObservationSeconds(0);
            String separateListener = listener(separateSource, 60, "IMPORT_EXISTING");
            assertThat(manualScan(separateListener).get("created_count")).isEqualTo(1);
            assertConflict(() -> listeners.save(listener, 3, "renamed", source, "incoming/archive", 120, "BASELINE_ONLY", false));
            listeners.save(listener, 3, "renamed", source, "incoming/archive", 120, "BASELINE_ONLY", true);
            assertThat(listeners.form(listener).get("revision")).isEqualTo(2);
            assertThat(listeners.form(listener).get("source_id")).isEqualTo(source);
            Files.writeString(area.resolve("archive/new.bin"), "new version");
            assertThat(manualScan(listener).get("created_count")).isEqualTo(1);
            assertThat(listeners.form(listener).get("initialized_at")).isEqualTo(initialized);
            endpoints.toggle(endpoint, 1);
            assertThatThrownBy(() -> listeners.manual(listener)).hasMessageContaining("ENDPOINT_DISABLED");
            assertThatThrownBy(() -> listenerRequest(listener, endpoint, null)).hasMessageContaining("ENDPOINT_DISABLED");
        }
    }

    @ParameterizedTest @EnumSource(EndpointRevision.Protocol.class)
    void overlappingListenersAndApiShareActualDownloadAcrossSourceAliases(EndpointRevision.Protocol protocol) throws Exception {
        clock.time.set(Instant.now());
        Files.createDirectories(remoteRoot.resolve("incoming"));
        Files.writeString(remoteRoot.resolve("incoming/a.bin"), "payload across entrances");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, protocol, ProtocolServers.Mode.NORMAL)) {
            String folder = source(server, protocol, "FILE", "incoming");
            String endpoint = configured(folder).getEndpointId();
            configured(folder).setScanDepth(1);
            configured(folder).setFilePattern("*.bin");
            configured(folder).setMinimumObservationSeconds(0);
            String file = sourceOn(endpoint, "FILE", "incoming/a.bin");
            configured(file).setMinimumObservationSeconds(0);
            String consumer = sourceOn(endpoint, "FILE", "incoming/a.bin");
            configured(consumer).setRecipientId("other");
            configured(consumer).setMinimumObservationSeconds(0);
            String primary = listener(folder, 60, "IMPORT_EXISTING");
            String overlap = listener(file, 60, "IMPORT_EXISTING");
            String separate = listener(consumer, 60, "IMPORT_EXISTING");
            listeners.toggle(overlap, 1); listeners.toggle(separate, 1);
            String apiTask = listenerRequest(primary, endpoint, "incoming/a.bin");
            assertThat(tasks.get("pull", apiTask).triggerType).isEqualTo("API");
            assertThat(tasks.get("pull", apiTask).listenerId).isEqualTo(primary);
            assertThatThrownBy(() -> listenerRequest(primary, endpoint, "incoming-sibling/a.bin")).hasMessageContaining("LISTENER_SCOPE_MISMATCH");
            assertThatThrownBy(() -> listenerRequest(primary, endpoint, "incoming/a.txt")).hasMessageContaining("LISTENER_SCOPE_MISMATCH");
            mvc.perform(post("/api/transfers/tasks").header("Authorization", "Bearer other-test-token").contentType("application/json")
                    .content(listenerJson(primary, endpoint, null).toString())).andExpect(status().isForbidden());

            AtomicInteger reads = new AtomicInteger();
            CountDownLatch entered = new CountDownLatch(1), compete = new CountDownLatch(1);
            doAnswer(call -> {
                RemoteFileClient real = (RemoteFileClient) call.callRealMethod();
                RemoteFileClient remote = mock(RemoteFileClient.class, org.mockito.AdditionalAnswers.delegatesTo(real));
                doAnswer(stat -> {
                    if (!Thread.currentThread().getName().equals("pull-listener-scan") && entered.getCount() > 0) {
                        entered.countDown();
                        if (!compete.await(10, TimeUnit.SECONDS)) throw new java.io.IOException("TEST_RACE_TIMEOUT");
                    }
                    return real.lstat("/incoming/a.bin");
                }).when(remote).lstat("/incoming/a.bin");
                doAnswer(read -> {
                    reads.incrementAndGet(); real.read(read.getArgument(0), read.getArgument(1)); return null;
                }).when(remote).read(eq("/incoming/a.bin"), any());
                return remote;
            }).when(clients).connect(any());
            CompletableFuture<Void> api = CompletableFuture.runAsync(() -> uncheckedRun(apiTask, pulls));
            try {
                assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                listeners.manual(primary);
                clock.advance(1);
                db.update("UPDATE pull_listener SET next_run_at=? WHERE id=?", java.sql.Timestamp.from(clock.instant().minusSeconds(2)), primary);
                pollListeners();
            } finally { compete.countDown(); }
            api.get(20, TimeUnit.SECONDS);
            await(() -> "SUCCEEDED".equals(latest(primary).get("status")));
            String shared = transferId(apiTask);
            if (shared == null) {
                runOnce(apiTask, pulls);
                shared = transferId(apiTask);
            }
            assertThat(shared).isNotBlank();
            assertThat(count("pull_transfer", "endpoint_id", endpoint)).isEqualTo(1);
            assertThat(reads).hasValue(1);
            assertThat(listenerTasks(primary)).hasSize(1);
            String canonicalKey = db.queryForObject("SELECT delivery_key FROM pull_transfer WHERE id=?", String.class, shared);
            String legacyKey = UUID.randomUUID().toString().replace("-", "").repeat(2);
            db.update("INSERT INTO pull_delivery_key(delivery_key,current_transfer_id) VALUES (?,?)", legacyKey, shared);
            db.update("UPDATE pull_transfer SET delivery_key=? WHERE id=?", legacyKey, shared);
            db.update("DELETE FROM pull_delivery_key WHERE delivery_key=?", canonicalKey);
            assertThat(manualScan(overlap).get("reused_count")).isEqualTo(1);
            assertThat(listenerTasks(overlap)).isEqualTo(listenerTasks(primary));
            assertThat(db.queryForObject("SELECT current_transfer_id FROM pull_delivery_key WHERE delivery_key=?", String.class, canonicalKey)).isEqualTo(shared);
            assertThat(manualScan(separate).get("created_count")).isEqualTo(1);
            runOnce(listenerTasks(separate).get(0), pulls, "other");
            assertThat(count("pull_transfer", "endpoint_id", endpoint)).isEqualTo(2);
            assertThat(reads).hasValue(2);

            var page = mvc.perform(get("/config/listeners")).andExpect(status().isOk()).andReturn();
            assertThat(page.getResponse().getContentAsString(java.nio.charset.StandardCharsets.UTF_8)).contains("拉取监听", "运行记录");
            mvc.perform(get("/config/listeners/new")).andExpect(status().isOk());
            mvc.perform(get("/config/listeners/" + primary)).andExpect(status().isOk());
            mvc.perform(get("/config/listeners/" + primary + "/runs")).andExpect(status().isOk());
            mvc.perform(get("/config/listeners/" + primary + "/runs/" + latest(primary).get("id") + "/tasks")).andExpect(status().isOk());
            mvc.perform(post("/config/listeners/" + primary + "/run")).andExpect(status().isForbidden());
            var session = (org.springframework.mock.web.MockHttpSession) page.getRequest().getSession(false);
            String csrf = (String) session.getAttribute(com.siwei.web.EndpointCsrfFilter.TOKEN);
            mvc.perform(post("/config/listeners/" + primary + "/toggle").session(session).param("_csrf", csrf)
                    .param("editVersion", "1")).andExpect(redirectedUrl("/config/listeners"));
            mvc.perform(post("/config/listeners/" + primary + "/run").session(session).param("_csrf", csrf))
                    .andExpect(redirectedUrl("/config/listeners/" + primary + "/runs"));
            assertThat(listeners.form(primary)).containsEntry("enabled", false).containsEntry("manual_pending", true);
        }
    }

    @Test void aLiveLargeDownloadLeavesScansAndQueuedEndpointWorkIndependent() throws Exception {
        clock.time.set(Instant.now());
        Path ftpRoot = Files.createDirectories(remoteRoot.resolve("ftp"));
        Files.write(ftpRoot.resolve("large.bin"), new byte[16 * 1024 * 1024]);
        Files.writeString(ftpRoot.resolve("small.bin"), "queued");
        try (ProtocolServers ftp = new ProtocolServers(ftpRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL);
             ProtocolServers sftp = new ProtocolServers(remoteRoot.resolve("sftp"), EndpointRevision.Protocol.SFTP, ProtocolServers.Mode.NORMAL)) {
            String large = source(ftp, EndpointRevision.Protocol.FTP, "FILE", "large.bin");
            String endpoint = configured(large).getEndpointId();
            EndpointForm form = endpoints.form(endpoint); form.concurrency = 1;
            endpoints.save(endpoint, form);
            String small = sourceOn(endpoint, "FILE", "small.bin");
            String remote = source(sftp, EndpointRevision.Protocol.SFTP, "FILE", "sample.bin");
            for (String source : List.of(large, small, remote)) configured(source).setMinimumObservationSeconds(0);
            String primary = listener(large, 90, "IMPORT_EXISTING");
            manualScan(primary);
            String downloading = listenerTasks(primary).get(0);
            CountDownLatch bytesArrived = new CountDownLatch(1), resume = new CountDownLatch(1);
            doAnswer(call -> {
                RemoteFileClient real = (RemoteFileClient) call.callRealMethod();
                if (!endpoint.equals(call.<EndpointRevision>getArgument(0).endpointId)) return real;
                RemoteFileClient client = mock(RemoteFileClient.class, org.mockito.AdditionalAnswers.delegatesTo(real));
                doAnswer(read -> {
                    java.io.OutputStream output = read.getArgument(1);
                    real.read("/large.bin", new java.io.FilterOutputStream(output) {
                        @Override public void write(byte[] bytes, int offset, int length) throws java.io.IOException {
                            if (bytesArrived.getCount() > 0) {
                                bytesArrived.countDown();
                                try {
                                    if (!resume.await(10, TimeUnit.SECONDS)) throw new java.io.IOException("TEST_DOWNLOAD_TIMEOUT");
                                } catch (InterruptedException error) {
                                    Thread.currentThread().interrupt();
                                    throw new java.io.IOException(error);
                                }
                            }
                            out.write(bytes, offset, length);
                        }
                    });
                    return null;
                }).when(client).read(eq("/large.bin"), any());
                return client;
            }).when(clients).connect(any());
            CompletableFuture<Void> transfer = CompletableFuture.runAsync(() -> uncheckedRun(downloading, pulls));
            String queued;
            try {
                assertThat(bytesArrived.await(10, TimeUnit.SECONDS)).isTrue();
                String sibling = listener(small, 60, "IMPORT_EXISTING");
                String other = listener(remote, 60, "IMPORT_EXISTING");
                manualScan(sibling);
                queued = listenerTasks(sibling).get(0);
                assertThat(taskDb.claimTask(queued, UUID.randomUUID().toString())).isZero();
                assertThat(state(queued)).isEqualTo("QUEUED");
                manualScan(other);
                runOnce(listenerTasks(other).get(0), pulls);
                assertThat(state(listenerTasks(other).get(0))).isEqualTo("WAITING_ACCEPTANCE");
                assertThat(state(downloading)).isEqualTo("RUNNING");
            } finally { resume.countDown(); }
            transfer.get(20, TimeUnit.SECONDS);
            runOnce(queued, pulls);
            assertThat(state(queued)).isEqualTo("WAITING_ACCEPTANCE");
            assertThat(Files.size(pending(transferId(downloading)).resolve("large.bin"))).isEqualTo(16L * 1024 * 1024);
        }
    }

    @Test void incompatibleRulesRequireAnExplicitScopeChangeAndFailedDeliveriesKeepTheirTask() throws Exception {
        clock.time.set(Instant.now());
        Files.createDirectories(remoteRoot.resolve("incoming"));
        Files.writeString(remoteRoot.resolve("incoming/a.bin"), "actual content");
        Files.writeString(remoteRoot.resolve("incoming/a.bin._manifest.json"),
                "{\"version\":\"1\",\"files\":{\"a.bin\":\"" + "0".repeat(64) + "\"}}");
        try (ProtocolServers ftp = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            String manifest = source(ftp, EndpointRevision.Protocol.FTP, "FILE", "incoming/a.bin");
            var source = configured(manifest);
            source.setManifestName("_manifest.json"); source.setValidation("HASH");
            String listener = listener(manifest, 60, "IMPORT_EXISTING");
            String incompatible = sourceOn(source.getEndpointId(), "FILE", "incoming");
            configured(incompatible).setScanDepth(1);
            configured(incompatible).setValidation("HASH");
            configured(incompatible).setManifestName("_different.json");
            assertThatThrownBy(() -> listener(incompatible, 60, "IMPORT_EXISTING")).hasMessageContaining("OVERLAPPING_VERSION_RULES");
            pullSettings.getSources().remove(incompatible);
            manualScan(listener);
            String task = listenerTasks(listener).get(0);
            runOnce(task, pulls);
            assertThat(state(task)).isEqualTo("FAILED");
            assertThat(manualScan(listener).get("reused_count")).isEqualTo(1);
            assertThat(listenerTasks(listener)).containsExactly(task);
            assertThat(count("pull_transfer", "endpoint_id", source.getEndpointId())).isEqualTo(1);
        }
    }

    @ParameterizedTest @EnumSource(EndpointRevision.Protocol.class)
    void filePullIsTechnicallyPassedButOnlyReceiptCompletesTask(EndpointRevision.Protocol protocol) throws Exception {
        try (ProtocolServers server = new ProtocolServers(remoteRoot, protocol, ProtocolServers.Mode.READ_ONLY)) {
            String source = source(server, protocol, "FILE", "sample.bin");
            String task = accept(source);
            awaitPublished(task);
            String id = transferId(task);
            assertThat(state(task)).isEqualTo("WAITING_ACCEPTANCE");
            assertThat(Files.readAllBytes(pending(id).resolve("sample.bin"))).isEqualTo(Files.readAllBytes(remoteRoot.resolve("sample.bin")));
            assertThat(Files.isRegularFile(pending(id).resolve(".sharing-complete"))).isTrue();
            assertThat(tasks.get("pull", task).downloadedBytes).isEqualTo(Files.size(remoteRoot.resolve("sample.bin")));
            assertThatThrownBy(() -> store.fileProgress(id, "sample.bin", "late-claim", 0, null, false)).hasMessageContaining("DELIVERY_CLAIM_LOST");
            assertThat(db.queryForObject("SELECT verified FROM pull_file WHERE transfer_id=?", Boolean.class, id)).isTrue();
            var file = tasks.files("pull", task, 0, 50);
            assertThat(file.get("totalFiles")).isEqualTo(1L);
            assertThat(db.queryForObject("SELECT expected_hash FROM pull_file WHERE transfer_id=?", String.class, id)).isNull();
            assertThat(db.queryForObject("SELECT version_strength FROM pull_transfer WHERE id=?", String.class, id)).isEqualTo("WEAK");
            assertThat(db.queryForObject("SELECT count(*) FROM pull_listener_revision WHERE endpoint_id=?", Integer.class,
                    pullSettings.getSources().get(source).getEndpointId())).isZero();
            JsonNode notice = notice(id);
            assertThat(notice.path("validationLevel").asText()).isEqualTo("BASIC");
            assertThat(notice.path("dataLocation").asText()).startsWith("https://business.example/data/pending/");
            mvc.perform(post("/api/sharing/pull/confirmations").header("Authorization", "Bearer pull-test-token")
                    .contentType("application/json").content(receipt(id, "ACCEPTED").toString())).andExpect(status().isOk());
            assertThat(state(task)).isEqualTo("SUCCEEDED");
            assertThat(Files.exists(pending(id))).isTrue();
        }
    }

    @ParameterizedTest @EnumSource(EndpointRevision.Protocol.class)
    void directoryHashPullPreservesStructureAndEmptyDirectories(EndpointRevision.Protocol protocol) throws Exception {
        Path unit = remoteRoot.resolve("package"); Files.createDirectories(unit.resolve("nested")); Files.createDirectories(unit.resolve("empty"));
        Files.writeString(unit.resolve("nested/data.zip"), "archive stays a file");
        manifest(unit, Map.of("nested/data.zip", hash("archive stays a file")), "v1");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, protocol, ProtocolServers.Mode.READ_ONLY)) {
            String source = source(server, protocol, "PACKAGE_DIR", "package");
            var c = pullSettings.getSources().get(source); c.setValidation("HASH"); c.setManifestName("_manifest.json"); c.setReadyName("_READY");
            Files.writeString(unit.resolve("_READY"), hash(Files.readString(unit.resolve("_manifest.json"))));
            String task = accept(source); awaitPublished(task); String id = transferId(task);
            assertThat(Files.readString(pending(id).resolve("nested/data.zip"))).isEqualTo("archive stays a file");
            assertThat(Files.isDirectory(pending(id).resolve("empty"))).isTrue();
            assertThat(Files.exists(pending(id).resolve("_manifest.json"))).isFalse();
            assertThat(notice(id).path("totalFiles").asInt()).isEqualTo(1);
            assertThat(notice(id).path("validationLevel").asText()).isEqualTo("HASH");
            assertThat(db.queryForObject("SELECT version_strength FROM pull_transfer WHERE id=?", String.class, id)).isEqualTo("STRONG");
        }
    }

    @Test void stableObservationAndMissingHashWaitOnSameApiTask() throws Exception {
        Files.createDirectories(remoteRoot.resolve("incoming")); Files.writeString(remoteRoot.resolve("incoming/a.bin"), "payload");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            String source = source(server, EndpointRevision.Protocol.FTP, "FILE", "incoming/a.bin");
            pullSettings.getSources().get(source).setStableObservations(3);
            String task = accept(source); runOnce(task, pulls);
            assertThat(state(task)).isEqualTo("WAITING_SOURCE");
            assertThat(count("pull_transfer", "owner_task_id", task)).isZero();
            Files.writeString(remoteRoot.resolve("incoming/a.bin"), "changed payload");
            runOnce(task, pulls);
            assertThat(db.queryForObject("SELECT stable_count FROM pull_candidate WHERE source_id=?", Integer.class, source)).isEqualTo(1);
            ageCandidate(source); runOnce(task, pulls); ageCandidate(source); runOnce(task, pulls);
            assertThat(state(task)).isEqualTo("WAITING_ACCEPTANCE");
            assertThat(count("pull_transfer", "owner_task_id", task)).isEqualTo(1);

            String strict = source(server, EndpointRevision.Protocol.FTP, "FILE", "incoming/a.bin");
            var cfg = pullSettings.getSources().get(strict); cfg.setValidation("HASH"); cfg.setManifestName("_manifest.json");
            String waiting = accept(strict); runOnce(waiting, pulls);
            assertThat(state(waiting)).isEqualTo("WAITING_SOURCE");
            assertThat(tasks.get("pull", waiting).deliveries.get(0).get("unit_error")).isEqualTo("MANIFEST_MISSING");
            assertThat(count("pull_transfer", "owner_task_id", waiting)).isZero();
            Files.writeString(remoteRoot.resolve("incoming/a.bin._manifest.json"), "{\"version\":\"1\",\"files\":{\"a.bin\":\"" + hash("changed payload") + "\"}}");
            runOnce(waiting, pulls);
            assertThat(state(waiting)).isEqualTo("WAITING_ACCEPTANCE");
        }
    }

    @Test void missingParentDirectoriesWaitWithinReadRootAndContinueOnTheSameTask() throws Exception {
        Files.createDirectories(remoteRoot.resolve("allowed"));
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            String source = source(server, EndpointRevision.Protocol.FTP, "FILE", "allowed/future/batch/a.bin");
            String endpoint = pullSettings.getSources().get(source).getEndpointId();
            EndpointForm restricted = server.form(EndpointRevision.Protocol.FTP);
            restricted.localTargetRef = "delivery"; restricted.readRoot = "/allowed"; restricted.readTestFile = null;
            restricted.revision = 1; endpoints.save(endpoint, restricted);
            pullSettings.getSources().get(source).setSourcePath("future/batch/a.bin");
            String task = accept(source); runOnce(task, pulls);
            assertThat(state(task)).isEqualTo("WAITING_SOURCE");
            assertThat(count("pull_transfer", "owner_task_id", task)).isZero();
            Files.createDirectories(remoteRoot.resolve("allowed/future/batch"));
            Files.writeString(remoteRoot.resolve("allowed/future/batch/a.bin"), "ready later");
            runOnce(task, pulls);
            assertThat(state(task)).isEqualTo("WAITING_ACCEPTANCE");
            assertThat(Files.readString(pending(transferId(task)).resolve("a.bin"))).isEqualTo("ready later");
        }
    }

    @Test void badHashNeverPublishesAndFinalFailureNoticeHasNoDeliveryLocation() throws Exception {
        Path unit = remoteRoot.resolve("bad"); Files.createDirectories(unit); Files.writeString(unit.resolve("a.bin"), "bad-data");
        manifest(unit, Map.of("a.bin", hash("gooddata")), "bad-v1");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.SFTP, ProtocolServers.Mode.NORMAL)) {
            String source = source(server, EndpointRevision.Protocol.SFTP, "PACKAGE_DIR", "bad");
            var cfg = pullSettings.getSources().get(source); cfg.setValidation("HASH"); cfg.setManifestName("_manifest.json");
            String task = accept(source); runOnce(task, pulls); String id = transferId(task);
            assertThat(state(task)).isEqualTo("FAILED");
            assertThat(tasks.get("pull", task).acceptanceStatus).isEqualTo("NOT_STARTED");
            assertThat(Files.exists(pending(id))).isFalse();
            JsonNode notice = notice(id);
            assertThat(notice.path("errorType").asText()).isEqualTo("HASH_MISMATCH");
            assertThat(notice.has("dataLocation")).isFalse(); assertThat(notice.has("validationLevel")).isFalse();
            assertThat(tasks.get("pull", task).downloadedBytes).isEqualTo(8L);
            assertThat(tasks.get("pull", task).verifiedBytes).isZero();
            assertThatThrownBy(() -> pulls.confirm("pull", receipt(id, "ACCEPTED"))).isInstanceOf(ResponseStatusException.class);
            String retry = accept(source); runOnce(retry, pulls);
            assertThat(transferId(retry)).isEqualTo(id);
            assertThat(db.queryForObject("SELECT attempts FROM pull_transfer WHERE id=?", Integer.class, id)).isEqualTo(1);
        }
    }

    @ParameterizedTest @EnumSource(EndpointRevision.Protocol.class)
    void sourceChangeDuringDownloadRestartsCandidateWithoutMixedPublication(EndpointRevision.Protocol protocol) throws Exception {
        Files.writeString(remoteRoot.resolve("moving.bin"), "original");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, protocol, ProtocolServers.Mode.NORMAL)) {
            String source = source(server, protocol, "FILE", "moving.bin"); String task = accept(source);
            AtomicBoolean changed = new AtomicBoolean();
            RemoteFileClientFactory changing = mock(RemoteFileClientFactory.class);
            when(changing.connect(any())).thenAnswer(invocation -> {
                RemoteFileClient actual = clients.connect(invocation.getArgument(0));
                RemoteFileClient remote = mock(RemoteFileClient.class, org.mockito.AdditionalAnswers.delegatesTo(actual));
                doAnswer(read -> {
                    actual.read(read.getArgument(0), read.getArgument(1));
                    if (changed.compareAndSet(false, true)) Files.writeString(remoteRoot.resolve("moving.bin"), "new changed contents");
                    return null;
                }).when(remote).read(anyString(), any());
                return remote;
            });
            PullProcessor changingPull = new PullProcessor(store, taskDb, json, revisions, changing, transferSettings, manager, clock);
            runOnce(task, changingPull);
            String old = db.queryForObject("SELECT id FROM pull_transfer WHERE owner_task_id=?", String.class, task);
            assertThat(state(task)).isEqualTo("WAITING_SOURCE");
            assertThat(Files.exists(pending(old))).isFalse();
            assertThat(db.queryForObject("SELECT status FROM pull_transfer WHERE id=?", String.class, old)).isEqualTo("SOURCE_CHANGED");
            runOnce(task, pulls); String current = transferId(task);
            assertThat(current).isNotEqualTo(old);
            assertThat(Files.readString(pending(current).resolve("moving.bin"))).isEqualTo("new changed contents");
        }
    }

    @Test void concurrentApiTasksReuseOneDeliveryAndDownload() throws Exception {
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            String source = source(server, EndpointRevision.Protocol.FTP, "FILE", "sample.bin");
            String first = accept(source), second = accept(source);
            AtomicInteger reads = new AtomicInteger();
            RemoteFileClientFactory counting = mock(RemoteFileClientFactory.class);
            when(counting.connect(any())).thenAnswer(invocation -> {
                RemoteFileClient actual = clients.connect(invocation.getArgument(0));
                RemoteFileClient remote = mock(RemoteFileClient.class, org.mockito.AdditionalAnswers.delegatesTo(actual));
                doAnswer(read -> { reads.incrementAndGet(); actual.read(read.getArgument(0), read.getArgument(1)); return null; }).when(remote).read(anyString(), any());
                return remote;
            });
            PullProcessor processor = new PullProcessor(store, taskDb, json, revisions, counting, transferSettings, manager, clock);
            ExecutorService pool = Executors.newFixedThreadPool(2);
            try {
                Future<?> a = pool.submit(() -> uncheckedRun(first, processor)), b = pool.submit(() -> uncheckedRun(second, processor));
                a.get(20, TimeUnit.SECONDS); b.get(20, TimeUnit.SECONDS);
            } finally { pool.shutdownNow(); }
            runOnce(first, pulls); runOnce(second, pulls);
            assertThat(transferId(first)).isEqualTo(transferId(second));
            assertThat(reads.get()).isEqualTo(1);
            assertThat(tasks.get("pull", first).notifications).hasSize(1);
            assertThat(tasks.get("pull", second).notifications).hasSize(1);
            pulls.confirm("pull", receipt(transferId(first), "ACCEPTED"));
            assertThat(state(first)).isEqualTo("SUCCEEDED"); assertThat(state(second)).isEqualTo("SUCCEEDED");
        }
    }

    @Test void changedObservationCanRefreezeTheSameManifestVersionWithoutReusingInvalidatedRound() throws Exception {
        Files.writeString(remoteRoot.resolve("changing.bin"), "fixed content");
        Files.writeString(remoteRoot.resolve("changing.bin._manifest.json"), "{\"version\":\"1\",\"files\":{\"changing.bin\":\"" + hash("fixed content") + "\"}}");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            String source = source(server, EndpointRevision.Protocol.FTP, "FILE", "changing.bin");
            var config = pullSettings.getSources().get(source); config.setValidation("HASH"); config.setManifestName("_manifest.json");
            String task = accept(source); AtomicBoolean changed = new AtomicBoolean();
            RemoteFileClientFactory changing = mock(RemoteFileClientFactory.class);
            when(changing.connect(any())).thenAnswer(invocation -> {
                RemoteFileClient actual = clients.connect(invocation.getArgument(0));
                RemoteFileClient remote = mock(RemoteFileClient.class, org.mockito.AdditionalAnswers.delegatesTo(actual));
                doAnswer(read -> {
                    actual.read(read.getArgument(0), read.getArgument(1));
                    if ("/changing.bin".equals(read.getArgument(0)) && changed.compareAndSet(false, true))
                        Files.setLastModifiedTime(remoteRoot.resolve("changing.bin"), java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis() + 5000));
                    return null;
                }).when(remote).read(anyString(), any());
                return remote;
            });
            runOnce(task, new PullProcessor(store, taskDb, json, revisions, changing, transferSettings, manager, clock));
            String old = db.queryForObject("SELECT id FROM pull_transfer WHERE owner_task_id=?", String.class, task);
            assertThat(db.queryForObject("SELECT status FROM pull_transfer WHERE id=?", String.class, old)).isEqualTo("SOURCE_CHANGED");
            runOnce(task, pulls); String current = transferId(task);
            assertThat(current).isNotEqualTo(old);
            assertThat(db.queryForObject("SELECT source_version FROM pull_transfer WHERE id=?", String.class, current))
                    .isEqualTo(db.queryForObject("SELECT source_version FROM pull_transfer WHERE id=?", String.class, old));
            assertThat(state(task)).isEqualTo("WAITING_ACCEPTANCE");
        }
    }

    @Test void differentConsumersAndTargetsHaveIndependentDeliveries() throws Exception {
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            String source = source(server, EndpointRevision.Protocol.FTP, "FILE", "sample.bin");
            String first = accept(source); runOnce(first, pulls);
            pullSettings.getSources().get(source).setRecipientId("other");
            String second = accept(source); runOnce(second, pulls);
            assertThat(transferId(second)).isNotEqualTo(transferId(first));
            Files.createDirectories(DELIVERY.resolve("alternate"));
            endpointSettings.getLocalRoots().put("delivery", DELIVERY.resolve("alternate").toString());
            try {
                String third = accept(source); runOnce(third, pulls);
                assertThat(transferId(third)).isNotIn(transferId(first), transferId(second));
                assertThat(Files.exists(DELIVERY.resolve("alternate/pending/" + transferId(third) + "/sample.bin"))).isTrue();
            } finally { endpointSettings.getLocalRoots().put("delivery", DELIVERY.toString()); }
        }
    }

    @ParameterizedTest @EnumSource(EndpointRevision.Protocol.class)
    void lostNotificationAckDoesNotBlockEarlyReceiptOrRedownload(EndpointRevision.Protocol protocol) throws Exception {
        try (ProtocolServers server = new ProtocolServers(remoteRoot, protocol, ProtocolServers.Mode.NORMAL)) {
            String source = source(server, protocol, "FILE", "sample.bin");
            String external = "lost-" + UUID.randomUUID(); lostAcks.add(external);
            String task = accept(source, external); runOnce(task, pulls); String id = transferId(task);
            await(() -> { dispatch(); return "RETRY_WAIT".equals(db.queryForObject("SELECT status FROM transfer_notification WHERE transfer_id=?", String.class, id)); });
            assertThat(state(task)).isEqualTo("WAITING_ACCEPTANCE");
            pulls.confirm("pull", receipt(id, "ACCEPTED"));
            assertThat(state(task)).isEqualTo("SUCCEEDED");
            assertThat(pulls.confirm("pull", receipt(id, "ACCEPTED")).get("outcome")).isEqualTo("DUPLICATE");
            lostAcks.remove(external);
            db.update("UPDATE transfer_notification SET next_attempt_at=now() WHERE transfer_id=?", id);
            await(() -> { dispatch(); return "ACKED".equals(db.queryForObject("SELECT status FROM transfer_notification WHERE transfer_id=?", String.class, id)); });
            assertThat(db.queryForObject("SELECT attempts FROM pull_transfer WHERE id=?", Integer.class, id)).isEqualTo(1);
        }
    }

    @ParameterizedTest @EnumSource(EndpointRevision.Protocol.class)
    void receiptsEnforceRecipientVersionPublishedStateAndTerminalConflictHistory(EndpointRevision.Protocol protocol) throws Exception {
        try (ProtocolServers server = new ProtocolServers(remoteRoot, protocol, ProtocolServers.Mode.NORMAL)) {
            String source = source(server, protocol, "FILE", "sample.bin");
            String task = accept(source); runOnce(task, pulls); String id = transferId(task);
            mvc.perform(post("/api/sharing/pull/confirmations").header("Authorization", "Bearer other-test-token")
                    .contentType("application/json").content(receipt(id, "ACCEPTED").toString())).andExpect(status().isForbidden()).andExpect(jsonPath("$.code").value("RECIPIENT_FORBIDDEN"));
            mvc.perform(post("/api/sharing/pull/confirmations").contentType("application/json").content(receipt(id, "ACCEPTED").toString())).andExpect(status().isUnauthorized());
            ObjectNode wrongVersion = receipt(id, "ACCEPTED").put("sourceVersion", "wrong");
            assertConflict(() -> pulls.confirm("pull", wrongVersion));
            pulls.confirm("pull", receipt(id, "ACCEPTED"));
            ObjectNode rejected = receipt(id, "REJECTED").put("reason", "bad quality").put("category", "SOURCE_QUALITY");
            assertConflict(() -> pulls.confirm("pull", rejected));
            assertThat(state(task)).isEqualTo("SUCCEEDED");
            assertThat(db.queryForObject("SELECT count(*) FROM pull_receipt WHERE transfer_id=? AND outcome='CONFLICT'", Integer.class, id)).isEqualTo(2);
        }
    }

    @ParameterizedTest @EnumSource(EndpointRevision.Protocol.class)
    void redeliveryProtectsOldRoundAndIndependentSourceVersions(EndpointRevision.Protocol protocol) throws Exception {
        Files.writeString(remoteRoot.resolve("round.bin"), "version one");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, protocol, ProtocolServers.Mode.NORMAL)) {
            String source = source(server, protocol, "FILE", "round.bin");
            String first = accept(source); runOnce(first, pulls); String old = transferId(first);
            String next = pulls.redeliver(old, "operator repaired local storage");
            assertThat(next).isNotEqualTo(old);
            assertThat(pulls.confirm("pull", receipt(old, "ACCEPTED")).get("outcome")).isEqualTo("HISTORICAL");
            assertThat(db.queryForObject("SELECT acceptance_status FROM pull_transfer WHERE id=?", String.class, next)).isEqualTo("NOT_STARTED");
            assertConflict(() -> pulls.confirm("pull", receipt(next, "ACCEPTED")));
            runOnce(first, pulls);
            assertThat(Files.exists(pending(old))).isTrue(); assertThat(Files.exists(pending(next))).isTrue();
            Files.writeString(remoteRoot.resolve("round.bin"), "version two is different");
            String second = accept(source); runOnce(second, pulls); String other = transferId(second);
            assertThat(other).isNotEqualTo(next);
            pulls.confirm("pull", receipt(next, "ACCEPTED")); pulls.confirm("pull", receipt(other, "ACCEPTED"));
            assertThat(state(first)).isEqualTo("SUCCEEDED"); assertThat(state(second)).isEqualTo("SUCCEEDED");
            assertThat(count("pull_task_transfer", "task_id", first)).isEqualTo(2);
        }
    }

    @Test void multiUnitTaskWaitsForEveryReceiptAndRejectsBadBusinessDataWithoutDownloadLoop() throws Exception {
        Files.createDirectories(remoteRoot.resolve("range"));
        Files.writeString(remoteRoot.resolve("range/a.bin"), "first"); Files.writeString(remoteRoot.resolve("range/b.bin"), "second");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.SFTP, ProtocolServers.Mode.NORMAL)) {
            String source = source(server, EndpointRevision.Protocol.SFTP, "FILE", "range"); pullSettings.getSources().get(source).setScanDepth(1);
            String task = accept(source); runOnce(task, pulls);
            var units = tasks.get("pull", task).deliveries;
            assertThat(units).hasSize(2);
            String first = (String) units.get(0).get("transfer_id"), second = (String) units.get(1).get("transfer_id");
            pulls.confirm("pull", receipt(first, "ACCEPTED"));
            assertThat(state(task)).isEqualTo("WAITING_ACCEPTANCE");
            db.update("UPDATE pull_transfer SET published_at=now()-interval '2 days' WHERE id=?", second);
            runOnce(task, pulls);
            assertThat(db.queryForObject("SELECT acceptance_reminded_at IS NOT NULL FROM pull_transfer WHERE id=?", Boolean.class, second)).isTrue();
            assertThat(state(task)).isEqualTo("WAITING_ACCEPTANCE");
            pulls.confirm("pull", receipt(second, "REJECTED").put("reason", "business quality rejected").put("category", "SOURCE_QUALITY"));
            assertThat(state(task)).isEqualTo("FAILED");
            String again = accept(source); runOnce(again, pulls);
            assertThat(state(again)).isEqualTo("FAILED");
            assertThat(db.queryForObject("SELECT sum(attempts) FROM pull_transfer WHERE id IN (?,?)", Integer.class, first, second)).isEqualTo(2);
        }
    }

    @Test void waitingUnitDoesNotBlockReadySiblingAndEmptyScanHasNoFakeDelivery() throws Exception {
        Files.createDirectories(remoteRoot.resolve("range"));
        Files.writeString(remoteRoot.resolve("range/a.bin"), "first"); Files.writeString(remoteRoot.resolve("range/b.bin"), "");
        Files.createDirectories(remoteRoot.resolve("empty"));
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            String source = source(server, EndpointRevision.Protocol.FTP, "FILE", "range"); pullSettings.getSources().get(source).setScanDepth(1);
            String task = accept(source); runOnce(task, pulls);
            assertThat(state(task)).isEqualTo("WAITING_SOURCE");
            assertThat(count("pull_transfer", "owner_task_id", task)).isEqualTo(1);
            String emptySource = source(server, EndpointRevision.Protocol.FTP, "FILE", "empty"); pullSettings.getSources().get(emptySource).setScanDepth(1);
            String empty = accept(emptySource); runOnce(empty, pulls);
            assertThat(state(empty)).isEqualTo("NO_NEW_DATA");
            assertThat(count("pull_transfer", "owner_task_id", empty)).isZero();
            assertThat(count("transfer_notification", "task_id", empty)).isZero();
        }
    }

    @ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void publicationRecoversAfterDatabaseResultLostWithoutReadingRemoteAgain(boolean manualRecheck) throws Exception {
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            String source = source(server, EndpointRevision.Protocol.FTP, "FILE", "sample.bin"); String task = accept(source);
            db.execute("CREATE FUNCTION fail_pull_notice() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.task_id='" + task
                    + "' THEN RAISE EXCEPTION 'injected outbox failure'; END IF; RETURN NEW; END $$");
            db.execute("CREATE TRIGGER fail_pull_notice BEFORE INSERT ON transfer_notification FOR EACH ROW EXECUTE PROCEDURE fail_pull_notice()");
            try { runOnce(task, pulls); }
            finally { db.execute("DROP TRIGGER fail_pull_notice ON transfer_notification"); db.execute("DROP FUNCTION fail_pull_notice()"); }
            String id = transferId(task);
            assertThat(db.queryForObject("SELECT status FROM pull_transfer WHERE id=?", String.class, id)).isEqualTo("PUBLISHING");
            assertThat(Files.isRegularFile(pending(id).resolve(".sharing-complete"))).isTrue();
            assertThat(count("transfer_notification", "task_id", task)).isZero();
            RemoteFileClientFactory counting = mock(RemoteFileClientFactory.class);
            when(counting.connect(any())).thenThrow(new java.io.IOException("OFFLINE"));
            PullProcessor recovery = new PullProcessor(store, taskDb, json, revisions, counting, transferSettings, manager, clock);
            if (manualRecheck) db.update("UPDATE transfer_task SET read_only_recheck=true WHERE id=?", task);
            else db.update("UPDATE pull_transfer SET next_run_at=now() WHERE id=?", id);
            runOnce(task, recovery);
            assertThat(state(task)).isEqualTo("WAITING_ACCEPTANCE"); assertThat(notice(id).path("dataLocation").asText()).contains(id);
            assertThat(db.queryForObject("SELECT count(*) FROM transfer_notification WHERE transfer_id=?", Integer.class, id)).isEqualTo(1);
            verifyNoInteractions(counting);
        }
    }

    @Test void networkRetriesKeepTransferIdAndStopAfterFourAttempts() throws Exception {
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            String source = source(server, EndpointRevision.Protocol.FTP, "FILE", "sample.bin"); String task = accept(source);
            AtomicInteger reads = new AtomicInteger(); RemoteFileClientFactory failing = mock(RemoteFileClientFactory.class);
            when(failing.connect(any())).thenAnswer(invocation -> {
                RemoteFileClient actual = clients.connect(invocation.getArgument(0));
                RemoteFileClient remote = mock(RemoteFileClient.class, org.mockito.AdditionalAnswers.delegatesTo(actual));
                doAnswer(read -> { reads.incrementAndGet(); throw new java.io.IOException("NETWORK_INTERRUPTED"); }).when(remote).read(anyString(), any());
                return remote;
            });
            PullProcessor processor = new PullProcessor(store, taskDb, json, revisions, failing, transferSettings, manager, clock);
            String id = null;
            for (int i = 0; i < 4; i++) {
                runOnce(task, processor); String current = transferId(task);
                if (id == null) id = current; else assertThat(current).isEqualTo(id);
                db.update("UPDATE pull_transfer SET next_run_at=now() WHERE id=?", id);
            }
            assertThat(state(task)).isEqualTo("FAILED"); assertThat(reads.get()).isEqualTo(4);
            assertThat(notice(id).has("dataLocation")).isFalse();
        }
    }

    @Test void incompleteUnexposedCopyCanRecoverButPublishedContentsCannotBeOverwritten() throws Exception {
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            String source = source(server, EndpointRevision.Protocol.FTP, "FILE", "sample.bin"); String task = accept(source);
            runOnce(task, pulls); String id = transferId(task);
            Path temporary = DELIVERY.resolve("tmp/" + id); Files.createDirectories(temporary);
            Files.copy(pending(id).resolve("sample.bin"), temporary.resolve("sample.bin"));
            Files.delete(pending(id).resolve(".sharing-complete")); Files.writeString(pending(id).resolve("sample.bin"), "partial copy");
            db.update("DELETE FROM transfer_notification WHERE transfer_id=?", id);
            db.update("UPDATE pull_transfer SET status='PUBLISHING',acceptance_status='NOT_STARTED',published_at=NULL,data_location=NULL WHERE id=?", id);
            runOnce(task, pulls);
            assertThat(state(task)).isEqualTo("WAITING_ACCEPTANCE");
            assertThat(Files.readAllBytes(pending(id).resolve("sample.bin"))).isEqualTo(Files.readAllBytes(remoteRoot.resolve("sample.bin")));
            assertThat(Files.exists(pending(id).resolve(".sharing-complete"))).isTrue();
            Files.writeString(pending(id).resolve("sample.bin"), "published data tampered");
            db.update("DELETE FROM transfer_notification WHERE transfer_id=?", id);
            db.update("UPDATE pull_transfer SET status='PUBLISHING',acceptance_status='NOT_STARTED',published_at=NULL,data_location=NULL WHERE id=?", id);
            runOnce(task, pulls);
            assertThat(db.queryForObject("SELECT status FROM pull_transfer WHERE id=?", String.class, id)).isEqualTo("PUBLISHING");
            assertThat(Files.readString(pending(id).resolve("sample.bin"))).isEqualTo("published data tampered");
            assertThat(count("transfer_notification", "task_id", task)).isZero();
        }
    }

    @Test void partialScanDoesNotAdvanceCandidateStability() throws Exception {
        Path unit = remoteRoot.resolve("partial"); Files.createDirectories(unit.resolve("nested"));
        Files.writeString(unit.resolve("a.bin"), "payload"); Files.writeString(unit.resolve("nested/b.bin"), "payload");
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            String source = source(server, EndpointRevision.Protocol.FTP, "PACKAGE_DIR", "partial");
            pullSettings.getSources().get(source).setStableObservations(3); String task = accept(source); runOnce(task, pulls);
            ageCandidate(source);
            RemoteFileClientFactory failing = mock(RemoteFileClientFactory.class);
            when(failing.connect(any())).thenAnswer(invocation -> {
                RemoteFileClient actual = clients.connect(invocation.getArgument(0));
                RemoteFileClient remote = mock(RemoteFileClient.class, org.mockito.AdditionalAnswers.delegatesTo(actual));
                doThrow(new java.io.IOException("INCOMPLETE_SCAN")).when(remote).list("/partial/nested"); return remote;
            });
            runOnce(task, new PullProcessor(store, taskDb, json, revisions, failing, transferSettings, manager, clock));
            assertThat(db.queryForObject("SELECT stable_count FROM pull_candidate WHERE source_id=?", Integer.class, source)).isEqualTo(1);
            runOnce(task, pulls);
            assertThat(db.queryForObject("SELECT stable_count FROM pull_candidate WHERE source_id=?", Integer.class, source)).isEqualTo(2);
            assertThat(count("pull_transfer", "owner_task_id", task)).isZero();
        }
    }

    @Test void configSnapshotsAndIntakeSwitchPreserveExistingQueryAndConfirmation() throws Exception {
        try (ProtocolServers server = new ProtocolServers(remoteRoot, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            String source = source(server, EndpointRevision.Protocol.FTP, "FILE", "sample.bin");
            String external = UUID.randomUUID().toString(), task = accept(source, external);
            pullSettings.getSources().get(source).setAccessBase("C:/not-business-accessible");
            assertThat(accept(source, external)).isEqualTo(task);
            assertThatThrownBy(() -> accept(source)).isInstanceOf(ResponseStatusException.class).satisfies(e -> assertThat(((ResponseStatusException) e).getReason()).isEqualTo("INVALID_PULL_CONFIG"));
            runOnce(task, pulls); String id = transferId(task);
            assertThat(notice(id).path("dataLocation").asText()).startsWith("https://business.example/");
            pullSettings.setIntakeEnabled(false);
            try {
                assertThatThrownBy(() -> accept(source)).isInstanceOf(ResponseStatusException.class).satisfies(e -> assertThat(((ResponseStatusException) e).getStatus()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE));
                assertThat(tasks.get("pull", task).deliveries).hasSize(1);
                pulls.confirm("pull", receipt(id, "ACCEPTED")); assertThat(state(task)).isEqualTo("SUCCEEDED");
            } finally { pullSettings.setIntakeEnabled(true); }
        }
    }

    private String source(ProtocolServers server, EndpointRevision.Protocol protocol, String type, String path) {
        EndpointForm form = server.form(protocol); form.localTargetRef = "delivery";
        form.concurrency = 2;
        String endpoint = endpoints.save(null, form);
        transferSettings.getCallers().get("pull").getEndpointIds().add(endpoint);
        transferSettings.getCallers().get("other").getEndpointIds().add(endpoint);
        return sourceOn(endpoint, type, path);
    }
    private String sourceOn(String endpoint, String type, String path) {
        String id = "source-" + UUID.randomUUID(); PullSettings.Source source = new PullSettings.Source();
        source.setEndpointId(endpoint); source.setSourcePath(path); source.setTargetRef("delivery");
        source.setUnitType(type); source.setRecipientId("pull"); source.setCallerIds(Set.of("pull"));
        source.setAccessBase("https://business.example/data"); source.setStableObservations(1); source.setObservationSeconds(3600);
        pullSettings.getSources().put(id, source); return id;
    }
    private PullSettings.Source configured(String source) { return pullSettings.getSources().get(source); }
    private String listener(String source, int period, String mode) {
        return listeners.save(null, 0, source, source, configured(source).getSourcePath(), period, mode, false);
    }
    private ObjectNode listenerJson(String listener, String endpoint, String path) {
        var request = json.createObjectNode().put("taskId", UUID.randomUUID().toString()).put("direction", "PULL")
                .put("endpointId", endpoint).put("listenerId", listener);
        if (path != null) request.put("sourcePath", path);
        return request;
    }
    private String listenerRequest(String listener, String endpoint, String path) {
        var request = listenerJson(listener, endpoint, path);
        return tasks.accept("pull", request, request.toString(), "JSON").taskRecordId;
    }
    private void pollListeners() {
        scanner.dispatch();
    }
    private Map<String, Object> latest(String listener) {
        return db.queryForList("SELECT * FROM pull_listener_run WHERE listener_id=? ORDER BY started_at DESC,id DESC LIMIT 1", listener)
                .stream().findFirst().orElseGet(Map::of);
    }
    private Map<String, Object> manualScan(String listener) throws Exception {
        Object previous = latest(listener).getOrDefault("id", "");
        clock.time.updateAndGet(time -> time.plusMillis(5));
        listeners.manual(listener);
        pollListeners();
        await(() -> {
            var row = latest(listener);
            if (row.isEmpty() || previous.equals(row.get("id"))) { pollListeners(); return false; }
            return !"SCANNING".equals(row.get("status"));
        });
        return latest(listener);
    }
    private List<String> listenerTasks(String listener) {
        return db.queryForList("SELECT task_id FROM pull_listener_run_task WHERE run_id=?", String.class, latest(listener).get("id"));
    }
    private int runCount(String listener) { return count("pull_listener_run", "listener_id", listener); }
    private int stability(String source, String path) {
        return db.queryForObject("SELECT stable_count FROM pull_candidate WHERE source_id=? AND source_path=?", Integer.class, source, path);
    }
    private String accept(String source) { return accept(source, UUID.randomUUID().toString()); }
    private String accept(String source, String external) {
        var cfg = pullSettings.getSources().get(source);
        ObjectNode request = json.createObjectNode().put("taskId", external).put("direction", "PULL").put("endpointId", cfg.getEndpointId())
                .put("sourceId", source).put("sourcePath", cfg.getSourcePath()).put("targetPath", "delivery");
        return tasks.accept("pull", request, request.toString(), "JSON").taskRecordId;
    }
    private void runOnce(String task, PullProcessor processor) throws Exception {
        runOnce(task, processor, "pull");
    }
    private void runOnce(String task, PullProcessor processor, String caller) throws Exception {
        db.update("UPDATE transfer_task SET next_run_at=now() WHERE id=?", task);
        String claim = UUID.randomUUID().toString();
        if (taskDb.claimTask(task, claim) != 1) {
            await(() -> !"RUNNING".equals(state(task))); return;
        }
        processor.process(tasks.get(caller, task)); processor.finish(task, claim);
    }
    private void uncheckedRun(String task, PullProcessor processor) { try { runOnce(task, processor); } catch (Exception e) { throw new RuntimeException(e); } }
    private void dispatch() {
        notifications.poll();
    }
    private void awaitPublished(String task) throws Exception {
        worker.poll();
        await(() -> "WAITING_ACCEPTANCE".equals(state(task)));
    }
    private String state(String task) { return db.queryForObject("SELECT status FROM transfer_task WHERE id=?", String.class, task); }
    private String transferId(String task) { return db.queryForObject("SELECT transfer_id FROM pull_task_unit WHERE task_id=?", String.class, task); }
    private int count(String table, String field, String id) { return db.queryForObject("SELECT count(*) FROM " + table + " WHERE " + field + "=?", Integer.class, id); }
    private Path pending(String id) { return DELIVERY.resolve("pending/" + id); }
    private ObjectNode receipt(String id, String result) {
        return json.createObjectNode().put("transferId", id).put("sourceVersion", db.queryForObject("SELECT source_version FROM pull_transfer WHERE id=?", String.class, id)).put("result", result);
    }
    private JsonNode notice(String id) throws Exception { return json.readTree(db.queryForObject("SELECT payload FROM transfer_notification WHERE transfer_id=?", String.class, id)); }
    private void ageCandidate(String source) { db.update("UPDATE pull_candidate SET observed_at=now()-interval '2 hours' WHERE source_id=?", source); }
    private void manifest(Path unit, Map<String, String> files, String version) throws Exception {
        Files.writeString(unit.resolve("_manifest.json"), json.createObjectNode().put("version", version).set("files", json.valueToTree(files)).toString());
    }
    private static String hash(String value) throws Exception {
        byte[] digest = java.security.MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
        StringBuilder result = new StringBuilder(); for (byte b : digest) result.append(String.format("%02x", b & 255)); return result.toString();
    }
    private static void assertConflict(org.assertj.core.api.ThrowableAssert.ThrowingCallable call) {
        assertThatThrownBy(call).isInstanceOf(ResponseStatusException.class).satisfies(e -> assertThat(((ResponseStatusException) e).getStatus()).isEqualTo(HttpStatus.CONFLICT));
    }
    private static void await(java.util.function.BooleanSupplier condition) throws Exception { StreamingFtpServer.await(condition, Duration.ofSeconds(20)); }
    @AfterAll static void cleanup(@Autowired org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor scheduler) throws Exception {
        scheduler.destroy();
        if (receiver != null) receiver.stop(0);
        if (!SCHEMA.matches("pull_it_[a-f0-9]{32}")) throw new IllegalStateException("Unsafe schema cleanup");
        try (var connection = DriverManager.getConnection(URL, USER, PASSWORD); var statement = connection.createStatement()) { statement.execute("DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE"); }
        try (var paths = Files.walk(DELIVERY)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> { try { Files.delete(path); } catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); } });
        }
    }
}
