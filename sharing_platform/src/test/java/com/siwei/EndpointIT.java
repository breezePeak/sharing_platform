package com.siwei;

import java.nio.file.*;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.*;
import com.siwei.endpoint.*;
import com.siwei.web.EndpointCsrfFilter;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.*;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.*;
import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@AutoConfigureMockMvc
@DirtiesContext
class EndpointIT {
    private static final String URL = TestDatabase.requireUrl(System.getenv());
    private static final String USER = TestDatabase.require(System.getenv(), "TEST_DB_USERNAME");
    private static final String PASSWORD = TestDatabase.require(System.getenv(), "TEST_DB_PASSWORD");
    private static final String SCHEMA = "endpoint_it_" + UUID.randomUUID().toString().replace("-", "");
    private static final String KEY = Base64.getEncoder().encodeToString(new byte[32]);
    @DynamicPropertySource static void database(DynamicPropertyRegistry p) {
        p.add("spring.datasource.url", () -> URL); p.add("spring.datasource.username", () -> USER);
        p.add("spring.datasource.password", () -> PASSWORD); p.add("spring.datasource.hikari.schema", () -> SCHEMA);
        p.add("spring.flyway.default-schema", () -> SCHEMA); p.add("platform.endpoints.master-key", () -> KEY);
    }
    @Autowired EndpointService service;
    @Autowired EndpointRepository endpoints;
    @Autowired EndpointRevisionRepository revisions;
    @Autowired EndpointProbeRepository probes;
    @Autowired CredentialCipher cipher;
    @Autowired ProbeRunner runner;
    @Autowired ProbeStore store;
    @Autowired MockMvc mvc;
    @org.springframework.boot.web.server.LocalServerPort int port;
    @TempDir Path root;

    @ParameterizedTest @EnumSource(EndpointRevision.Protocol.class)
    void realProtocolProbeVerifiesAllStagesAndCleansItsOwnFiles(EndpointRevision.Protocol protocol) throws Exception {
        try (ProtocolServers server = new ProtocolServers(root, protocol, ProtocolServers.Mode.NORMAL)) {
            String id = service.save(null, server.form(protocol));
            EndpointProbe p = finish(runner.start(id, 1));
            assertThat(p.status).as(p.details).isEqualTo("COMPLETED");
            assertThat(p.canRead).as(p.details).isTrue(); assertThat(p.canWrite).as(p.details).isTrue();
            assertThat(p.verified).as(p.details).isTrue(); assertThat(p.canRename).as(p.details).isTrue();
            assertThat(p.isStrictPush()).as(p.details).isTrue(); assertThat(p.capacity).matches("UNKNOWN|BYTES:[0-9]+");
            assertThat(p.residuals).isEmpty();
            try (var files = Files.list(root.resolve("probe"))) { assertThat(files.count()).isZero(); }
            assertThat(Files.readString(root.resolve("sample.bin"))).contains("non-empty");
        }
    }

    @ParameterizedTest @EnumSource(EndpointRevision.Protocol.class)
    void readOnlyIsUsableAndWriteOnlyCannotClaimStrictPush(EndpointRevision.Protocol protocol) throws Exception {
        try (ProtocolServers server = new ProtocolServers(root, protocol, ProtocolServers.Mode.READ_ONLY)) {
            String id = service.save(null, server.form(protocol));
            EndpointProbe p = finish(runner.start(id, 1));
            assertThat(p.canRead).as(p.details).isTrue(); assertThat(p.canWrite).isFalse(); assertThat(p.isStrictPush()).isFalse();
            assertThat(endpoints.findById(id).orElseThrow().enabled).isTrue();
        }
        try (ProtocolServers server = new ProtocolServers(root, protocol, ProtocolServers.Mode.WRITE_ONLY)) {
            String id = service.save(null, server.form(protocol));
            EndpointProbe p = finish(runner.start(id, 1));
            assertThat(p.canRead).as(p.details).isFalse(); assertThat(p.canWrite).as(p.details).isTrue();
            assertThat(p.verified).isFalse(); assertThat(p.isStrictPush()).isFalse();
        }
    }

    @Test void ftpDataChannelFailureIsNotSuccessfulLoginAndStallIsBounded() throws Exception {
        try (ProtocolServers server = new ProtocolServers(root, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.BROKEN_DATA)) {
            String id = service.save(null, server.form(EndpointRevision.Protocol.FTP));
            EndpointProbe p = finish(runner.start(id, 1));
            assertThat(p.details).contains("CONNECT: OK", "LIST_FTP_425");
            assertThat(p.canRead).isFalse(); assertThat(p.canWrite).isFalse(); assertThat(p.isStrictPush()).isFalse();
        }
        try (ProtocolServers server = new ProtocolServers(root, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.STALL_READ)) {
            EndpointForm f = server.form(EndpointRevision.Protocol.FTP); f.timeoutSeconds = 1; f.writeRoot = null; f.probeDirectory = null;
            String id = service.save(null, f);
            long start = System.nanoTime();
            EndpointProbe p = finish(runner.start(id, 1));
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(10));
            assertThat(p.canRead).isFalse(); assertThat(p.details).containsAnyOf("TIMEOUT", "TIMED_OUT");
        }
    }

    @ParameterizedTest @EnumSource(EndpointRevision.Protocol.class)
    void cleanupFailureRecordsOwnedResidualsAndRenameFailureHasSeparateStage(EndpointRevision.Protocol protocol) throws Exception {
        try (ProtocolServers server = new ProtocolServers(root, protocol, ProtocolServers.Mode.CLEANUP_FAIL)) {
            String id = service.save(null, server.form(protocol));
            EndpointProbe p = finish(runner.start(id, 1));
            assertThat(p.residuals).as(p.details).contains("/probe/.sharing-probe-");
            assertThat(p.isStrictPush()).isFalse(); assertThat(p.details).contains("CLEANUP_FILE:");
        }
        try (ProtocolServers server = new ProtocolServers(root, protocol, ProtocolServers.Mode.RENAME_FAIL)) {
            String id = service.save(null, server.form(protocol));
            EndpointProbe p = finish(runner.start(id, 1));
            assertThat(p.canWrite).as(p.details).isTrue(); assertThat(p.verified).as(p.details).isTrue();
            assertThat(p.canRename).isFalse(); assertThat(p.isStrictPush()).isFalse(); assertThat(p.details).contains("RENAME:");
        }
    }

    @Test void overallDeadlineClosesLiveFtpSessionAndRetainsPossibleResiduals() throws Exception {
        try (ProtocolServers server = new ProtocolServers(root, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.SLOW_COMMANDS)) {
            EndpointForm f = server.form(EndpointRevision.Protocol.FTP);
            f.timeoutSeconds = 1; f.readRoot = null; f.readTestFile = null;
            String id = service.save(null, f);
            EndpointProbe p = finish(runner.start(id, 1));
            assertThat(p.status).as(p.details).isEqualTo("TIMED_OUT");
            assertThat(p.residuals).contains("/probe/.sharing-probe-");
            assertThat(p.isStrictPush()).isFalse();
        }
    }

    @Test void snapshotsSurviveRestartRetainOldCredentialsAndOldProbeIsStale() throws Exception {
        String id, originalRevision, ftpId;
        try (ProtocolServers server = new ProtocolServers(root, EndpointRevision.Protocol.SFTP, ProtocolServers.Mode.NORMAL)) {
            id = service.save(null, server.form(EndpointRevision.Protocol.SFTP));
            originalRevision = service.current(id).id;
            finish(runner.start(id, 1));
            EndpointForm f = service.form(id); f.name = "updated"; f.password = "new-password-canary";
            service.save(id, f);
            assertThat(cipher.decrypt(revisions.findById(originalRevision).orElseThrow())).isEqualTo(ProtocolServers.PASSWORD);
            assertThat(cipher.decrypt(service.current(id))).isEqualTo("new-password-canary");
            assertThat(service.form(id).password).isEmpty();
            EndpointForm retained = service.form(id); retained.name = "retained with blank password";
            service.save(id, retained);
            assertThat(cipher.decrypt(service.current(id))).isEqualTo("new-password-canary");
            assertThatThrownBy(() -> service.save(id, retained)).isInstanceOf(org.springframework.web.server.ResponseStatusException.class);
            mvc.perform(get("/config/endpoints")).andExpect(status().isOk())
                    .andExpect(content().string(containsString("检测已过期")))
                    .andExpect(content().string(not(containsString("new-password-canary"))))
                    .andExpect(content().string(not(containsString(ProtocolServers.PASSWORD))));
            service.clearCredential(id, 3);
            assertThat(service.current(id).hasSecret()).isFalse();
            assertThat(revisions.findById(originalRevision)).isPresent();
            service.toggle(id, 4);
            assertThat(endpoints.findById(id).orElseThrow().enabled).isFalse();
            assertThatThrownBy(() -> runner.start(id, 5)).isInstanceOf(IllegalArgumentException.class);
        }
        try (ProtocolServers server = new ProtocolServers(root, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            ftpId = service.save(null, server.form(EndpointRevision.Protocol.FTP));
        }
        String[] args = {"--server.port=0", "--spring.datasource.url=" + URL, "--spring.datasource.username=" + USER,
                "--spring.datasource.password=" + PASSWORD, "--spring.datasource.hikari.schema=" + SCHEMA,
                "--spring.flyway.default-schema=" + SCHEMA, "--platform.endpoints.master-key=" + KEY};
        try (var context = new SpringApplicationBuilder(App.class).profiles("test").run(args)) {
            EndpointService restarted = context.getBean(EndpointService.class);
            assertThat(restarted.current(id).revision).isEqualTo(5);
            assertThat(restarted.current(id).name).isEqualTo("retained with blank password");
            assertThat(restarted.current(ftpId).protocol).isEqualTo(EndpointRevision.Protocol.FTP);
            assertThat(context.getBean(CredentialCipher.class).decrypt(restarted.current(ftpId))).isEqualTo(ProtocolServers.PASSWORD);
            assertThat(context.getBean(EndpointRevisionRepository.class).findById(originalRevision)).isPresent();
        }
    }

    @Test void formsProtectAgainstForgeryOverpostingSecretEchoAndInvalidProtocolSettings() throws Exception {
        var page = mvc.perform(get("/config/endpoints/new")).andExpect(status().isOk()).andReturn();
        MockHttpSession session = (MockHttpSession) page.getRequest().getSession();
        String token = (String) session.getAttribute(EndpointCsrfFilter.TOKEN);
        mvc.perform(post("/config/endpoints").param("name", "attacker")).andExpect(status().isForbidden());
        mvc.perform(post("/config/endpoints").session(session).param("_csrf", "wrong")).andExpect(status().isForbidden());
        mvc.perform(post("/config/endpoints").session(session).param("_csrf", token).param("name", "valid")
                .param("host", "127.0.0.1").param("username", "tester").param("password", "secret-form-canary")
                .param("readRoot", "/").param("isolatedFtpRoot", "true").param("secret", "overpost"))
                .andExpect(status().isBadRequest()).andExpect(content().string(not(containsString("secret-form-canary"))));
        try (ProtocolServers server = new ProtocolServers(root, EndpointRevision.Protocol.SFTP, ProtocolServers.Mode.NORMAL)) {
            EndpointForm f = server.form(EndpointRevision.Protocol.SFTP); f.fingerprint = "";
            assertThatThrownBy(() -> service.save(null, f)).isInstanceOf(IllegalArgumentException.class);
            f.fingerprint = server.fingerprint; f.readTestFile = "../escape";
            assertThatThrownBy(() -> service.save(null, f)).isInstanceOf(IllegalArgumentException.class);
            f.readTestFile = "sample.bin"; f.localTargetRef = "unconfigured-root";
            assertThatThrownBy(() -> service.save(null, f)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test void interruptedProbeRetainsEvidenceAndLateWorkerCannotOverwriteTimeout() throws Exception {
        try (ProtocolServers server = new ProtocolServers(root, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL)) {
            String id = service.save(null, server.form(EndpointRevision.Protocol.FTP));
            EndpointProbe p = store.create(id, 1);
            store.progress(p.id, "WRITE_STARTED", "/probe/.sharing-probe-" + "a".repeat(32));
            assertThatThrownBy(() -> store.create(id, 1)).isInstanceOf(org.springframework.web.server.ResponseStatusException.class);
            store.stop(p.id, "TIMED_OUT"); p.status = "COMPLETED"; p.verified = true; p.residuals = "";
            store.finish(p);
            assertThat(probes.findById(p.id).orElseThrow().status).isEqualTo("TIMED_OUT");
            assertThat(probes.findById(p.id).orElseThrow().residuals).isNotEmpty();
            EndpointProbe interrupted = store.create(id, 1); store.recoverInterrupted();
            assertThat(probes.findById(interrupted.id).orElseThrow().status).isEqualTo("INTERRUPTED");
        }
    }

    @Test void realHttpSessionCanSaveAnEscapedFormAndRejectsMissingCsrf() throws Exception {
        var client = java.net.http.HttpClient.newBuilder().cookieHandler(new java.net.CookieManager()).build();
        String base = "http://127.0.0.1:" + port;
        var page = client.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create(base + "/config/endpoints/new")).build(),
                java.net.http.HttpResponse.BodyHandlers.ofString());
        assertThat(page.statusCode()).isEqualTo(200);
        var matcher = java.util.regex.Pattern.compile("name=\"_csrf\" value=\"([^\"]+)\"").matcher(page.body());
        assertThat(matcher.find()).isTrue();
        String body = "_csrf=" + matcher.group(1) + "&name=%3Cscript%3Ewire-test%3C%2Fscript%3E&host=127.0.0.1&port=21"
                + "&username=tester&password=runtime-form-secret&readRoot=%2F&isolatedFtpRoot=true&enabled=true";
        var saved = client.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create(base + "/config/endpoints"))
                .header("Content-Type", "application/x-www-form-urlencoded").POST(java.net.http.HttpRequest.BodyPublishers.ofString(body)).build(),
                java.net.http.HttpResponse.BodyHandlers.ofString());
        assertThat(saved.statusCode()).isEqualTo(302);
        var list = client.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create(base + "/config/endpoints")).build(),
                java.net.http.HttpResponse.BodyHandlers.ofString());
        assertThat(list.statusCode()).isEqualTo(200);
        assertThat(list.body()).contains("&lt;script&gt;wire-test&lt;/script&gt;").doesNotContain("runtime-form-secret", "<script>wire-test");
        var forged = client.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create(base + "/config/endpoints"))
                .header("Content-Type", "application/x-www-form-urlencoded").POST(java.net.http.HttpRequest.BodyPublishers.ofString("name=forged")).build(),
                java.net.http.HttpResponse.BodyHandlers.ofString());
        assertThat(forged.statusCode()).isEqualTo(403);
    }

    private EndpointProbe finish(String probeId) {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            EndpointProbe p = probes.findById(probeId).orElseThrow();
            if (!p.status.equals("RUNNING")) return p;
            try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); }
        }
        throw new AssertionError("Probe exceeded test deadline: " + probeId);
    }
    @AfterAll static void cleanup() throws Exception {
        if (!SCHEMA.matches("endpoint_it_[a-f0-9]{32}")) throw new IllegalStateException("Unsafe schema cleanup");
        try (var c = DriverManager.getConnection(URL, USER, PASSWORD); var s = c.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE");
        }
    }
}
