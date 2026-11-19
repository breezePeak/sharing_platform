package com.siwei;

import com.siwei.web.EndpointCsrfFilter;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.DriverManager;
import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.mock.web.*;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.*;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@ActiveProfiles("test")
@DirtiesContext
class ConsoleIT {
    private static final String URL = TestDatabase.requireUrl(System.getenv());
    private static final String USER = TestDatabase.require(System.getenv(), "TEST_DB_USERNAME");
    private static final String PASSWORD = TestDatabase.require(System.getenv(), "TEST_DB_PASSWORD");
    private static final String SCHEMA = "console_it_" + UUID.randomUUID().toString().replace("-", "");

    @DynamicPropertySource static void properties(DynamicPropertyRegistry p) {
        p.add("spring.datasource.url", () -> URL); p.add("spring.datasource.username", () -> USER);
        p.add("spring.datasource.password", () -> PASSWORD); p.add("spring.datasource.hikari.schema", () -> SCHEMA);
        p.add("spring.flyway.default-schema", () -> SCHEMA);
    }

    @Autowired MockMvc mvc;
    @Autowired JdbcTemplate db;
    @Autowired com.siwei.transfer.TransferJpa transferDb;
    @MockBean org.springframework.scheduling.TaskScheduler scheduler;
    @LocalServerPort int port;

    String endpoint, revision;

    @BeforeEach void seed() {
        endpoint = id(); revision = id();
        db.update("INSERT INTO endpoint(id,revision,enabled) VALUES (?,1,true)", endpoint);
        db.update("INSERT INTO endpoint_revision(id,endpoint_id,revision,name,protocol,host,port,username,auth,read_root,write_root,"
                + "timeout_seconds,concurrency,passive,isolated_ftp_root,created_at) VALUES (?,?,1,'旧连接','FTP','ftp.snapshot.invalid',21,"
                + "'test-login','PASSWORD','/source','/target',2,1,true,true,now())", revision, endpoint);
    }

    @Test void realTemplatesSeparateHealthProgressAndHistoryAndPreserveLargeBytes() throws Exception {
        mvc.perform(get("/services")).andExpect(status().isOk()).andExpect(content().string(containsString("连接观测未知")));
        db.update("INSERT INTO endpoint_probe(id,endpoint_id,revision_id,started_at,finished_at,status,can_read,can_write,verified,can_rename,details,residuals,capacity) "
                + "VALUES (?,?,?,now(),now(),'COMPLETED',true,true,true,true,'CONNECT: OK','','UNKNOWN')", id(), endpoint, revision);
        String task = task("PUSH", "RUNNING", "VERIFYING");
        db.update("UPDATE transfer_task SET push_manifest_sha256=repeat('b',64),push_target_root='/target/frozen',current_file='p/25.bin' WHERE id=?", task);
        for (int i = 0; i < 26; i++) {
            long bytes = i == 25 ? 9007199254740993L : 0;
            String name = i == 25 ? "zz-<script>alert(1)</script>.bin" : String.format("%02d.bin", i);
            db.update("INSERT INTO transfer_file(task_id,product_id,relative_path,size_bytes,sha256,source_modified_millis,uploaded_bytes,effective_uploaded_bytes,verified)"
                    + " VALUES (?,'p',?,?,repeat('a',64),0,?,?,?)", task, name, bytes, bytes, bytes, i < 25);
        }
        mvc.perform(get("/tasks/" + task)).andExpect(status().isOk()).andExpect(content().string(containsString("目标核验中")))
                .andExpect(content().string(containsString("9007199254740993"))).andExpect(content().string(containsString("value=\"100\"")))
                .andExpect(content().string(containsString("下一页"))).andExpect(content().string(not(containsString("alert(1)"))));
        mvc.perform(get("/tasks/" + task).param("page", "1")).andExpect(status().isOk())
                .andExpect(content().string(containsString("&lt;script&gt;alert(1)&lt;/script&gt;")));
        db.update("UPDATE transfer_file SET effective_uploaded_bytes=1 WHERE task_id=? AND size_bytes>0", task);
        mvc.perform(get("/tasks/" + task + "/status")).andExpect(status().isOk())
                .andExpect(content().string(containsString("value=\"0\""))).andExpect(content().string(containsString("data-task-summary")));
        String newRevision = id();
        db.update("INSERT INTO endpoint_revision(id,endpoint_id,revision,name,protocol,host,port,username,auth,timeout_seconds,concurrency,passive,isolated_ftp_root,created_at)"
                + " VALUES (?,?,2,'新连接','FTP','ftp.current.invalid',21,'test-login','PASSWORD',2,1,true,true,now())", newRevision, endpoint);
        db.update("UPDATE endpoint SET enabled=false,revision=2 WHERE id=?", endpoint);
        db.update("UPDATE transfer_task SET status='FAILED',error_type='HASH_MISMATCH' WHERE id=?", task);
        mvc.perform(get("/tasks/" + task)).andExpect(status().isOk())
                .andExpect(content().string(containsString("ftp.snapshot.invalid"))).andExpect(content().string(containsString("HASH_MISMATCH")))
                .andExpect(content().string(not(containsString("ftp.current.invalid"))));
        mvc.perform(get("/services")).andExpect(status().isOk())
                .andExpect(content().string(containsString("COMPLETED"))).andExpect(content().string(containsString("旧配置")));
        mvc.perform(get("/tasks").param("endpoint", endpoint).param("status", "FAILED").param("direction", "PUSH"))
                .andExpect(status().isOk()).andExpect(content().string(containsString(task)));
        mvc.perform(get("/tasks").param("status", "invalid")).andExpect(status().isBadRequest());
        mvc.perform(get("/config/routes")).andExpect(status().isOk()).andExpect(content().string(containsString("来源与路由")));
    }

    @Test void showsPullAcceptanceAndPartialFailureAndAuditsSingleStatusCheckedOperations() throws Exception {
        String pull = task("PULL", "WAITING_ACCEPTANCE", "WAITING_ACCEPTANCE");
        db.update("UPDATE transfer_task SET pull_scan_complete=true WHERE id=?", pull);
        mvc.perform(get("/tasks/" + pull)).andExpect(status().isOk())
                .andExpect(content().string(containsString("下载及技术检查通过，待平台验收")))
                .andExpect(content().string(not(containsString("<progress"))));
        db.update("INSERT INTO pull_task_unit(task_id,source_path,status,error_type) VALUES (?,'/in/bad.bin','FAILED','HASH_MISMATCH')", pull);
        mvc.perform(get("/tasks/" + pull).param("view", "units")).andExpect(status().isOk())
                .andExpect(content().string(containsString("部分单元失败，其他单元处理中")))
                .andExpect(content().string(containsString("扫描或预检查中，待确定总量")));
        db.update("UPDATE transfer_task SET status='FAILED',acceptance_status='REJECTED' WHERE id=?", pull);
        mvc.perform(get("/tasks/" + pull)).andExpect(status().isOk()).andExpect(content().string(containsString("平台验收拒绝")));

        String push = task("PUSH", "SUCCEEDED", "NOTIFYING");
        String notice = id();
        db.update("INSERT INTO transfer_notification(id,task_id,stage,result_status,protocol,recipient_url,payload,payload_sha256,next_attempt_at,status,created_at) "
                + "VALUES (?,?,'COMPLETE','SUCCEEDED','JSON','http://127.0.0.1:1/notify','fixed',repeat('a',64),now(),'NEEDS_ATTENTION',now())", notice, push);
        var visit = mvc.perform(get("/tasks/" + push).param("view", "timeline")).andExpect(status().isOk())
                .andExpect(content().string(containsString("交付成功，完成通知待送达"))).andReturn();
        var session = (MockHttpSession) visit.getRequest().getSession(false);
        String csrf = (String) session.getAttribute(EndpointCsrfFilter.TOKEN);
        String nonce = id(), resend = "/tasks/" + push + "/notifications/" + notice + "/resend";
        mvc.perform(post(resend).session(session).param("nonce", nonce)).andExpect(status().isForbidden());
        for (int i = 0; i < 2; i++) mvc.perform(post(resend).session(session).param("_csrf", csrf).param("nonce", nonce)).andExpect(status().is3xxRedirection());
        assertThat(db.queryForObject("SELECT count(*) FROM console_operation WHERE object_id=? AND result='ACCEPTED'", Integer.class, notice)).isEqualTo(1);
        assertThat(db.queryForObject("SELECT payload FROM transfer_notification WHERE id=?", String.class, notice)).isEqualTo("fixed");
        assertThat(db.queryForObject("SELECT status FROM transfer_task WHERE id=?", String.class, push)).isEqualTo("SUCCEEDED");
        db.update("UPDATE transfer_notification SET status='ACKED' WHERE id=?", notice);
        mvc.perform(post(resend).session(session).param("_csrf", csrf).param("nonce", id())).andExpect(status().isConflict());
        String reconcile = task("PUSH", "RECONCILING", "PUBLISHING");
        db.update("UPDATE transfer_task SET push_publish_intent=true,reconcile_attempts=8,next_run_at=now() WHERE id=?", reconcile);
        assertThat(transferDb.claimTask(reconcile, id())).isZero();
        String recheck = "/tasks/" + reconcile + "/recheck";
        mvc.perform(get("/tasks/" + reconcile)).andExpect(status().isOk()).andExpect(content().string(containsString("发布结果待核对")))
                .andExpect(content().string(containsString("自动核对已到上限")));
        mvc.perform(post(recheck).session(session).param("_csrf", csrf).param("nonce", id())).andExpect(status().is3xxRedirection());
        assertThat(db.queryForObject("SELECT read_only_recheck FROM transfer_task WHERE id=?", Boolean.class, reconcile)).isTrue();
        assertThat(db.queryForObject("SELECT status FROM transfer_task WHERE id=?", String.class, reconcile)).isEqualTo("RECONCILING");
        mvc.perform(post(recheck).session(session).param("_csrf", csrf).param("nonce", id())).andExpect(status().isConflict());
        assertThat(transferDb.claimTask(reconcile, id())).isEqualTo(1);
        mvc.perform(get("/api/transfers/tasks/" + push)).andExpect(status().isUnauthorized());
    }

    @Test void consoleAuthenticationProtectsPageAndRefreshRoutes() throws Exception {
        var filter = new EndpointCsrfFilter("ops", "test-only-password", new MockEnvironment());
        var noAuth = new MockHttpServletRequest("GET", "/tasks/sample/status");
        var denied = new MockHttpServletResponse();
        filter.doFilter(noAuth, denied, new MockFilterChain());
        assertThat(denied.getStatus()).isEqualTo(401);
        assertThat(denied.getHeader("WWW-Authenticate")).startsWith("Basic");
        var allowed = new MockHttpServletRequest("GET", "/tasks/sample/status");
        allowed.addHeader("Authorization", "Basic " + Base64.getEncoder().encodeToString("ops:test-only-password".getBytes(StandardCharsets.UTF_8)));
        var accepted = new MockHttpServletResponse();
        filter.doFilter(allowed, accepted, new MockFilterChain());
        assertThat(accepted.getStatus()).isEqualTo(200);
        assertThat(allowed.getAttribute(EndpointCsrfFilter.ACTOR)).isEqualTo("ops");
        assertThat(accepted.getHeader("Cache-Control")).isEqualTo("no-store");
    }

    @Test @EnabledIfEnvironmentVariable(named = "TEST_BROWSER_EXE", matches = ".+")
    void chromiumRendersTheLiveConsoleAndStatusEndpoint() throws Exception {
        String task = task("PULL", "WAITING_ACCEPTANCE", "WAITING_ACCEPTANCE");
        Path target = Path.of("target").toAbsolutePath();
        Path output = target.resolve("console-browser.html"), errors = target.resolve("console-browser.log");
        Path profile = Files.createTempDirectory(target, "console-chrome-");
        Process browser = new ProcessBuilder(System.getenv("TEST_BROWSER_EXE"), "--headless=new", "--no-first-run", "--disable-gpu",
                "--disable-background-networking", "--no-proxy-server", "--disable-breakpad", "--user-data-dir=" + profile,
                "--dump-dom", "http://127.0.0.1:" + port + "/tasks/" + task)
                .redirectOutput(output.toFile()).redirectError(errors.toFile()).start();
        try {
            assertThat(browser.waitFor(40, TimeUnit.SECONDS)).isTrue();
            assertThat(browser.exitValue()).isZero();
            assertThat(Files.readString(output)).contains("下载及技术检查通过，待平台验收", "data-task-summary",
                    "/tasks/" + task + "/status", "/js/task-refresh.js");
        } finally { if (browser.isAlive()) browser.destroyForcibly(); }
    }

    private String task(String direction, String status, String phase) {
        String task = id();
        db.update("INSERT INTO transfer_task(id,caller_id,external_task_id,entry_type,direction,endpoint_id,endpoint_revision_id,notification_url,"
                + "canonical_request,raw_request,status,phase,next_run_at,created_at,updated_at) "
                + "VALUES (?,'test',?,'JSON',?,?,?,'http://127.0.0.1:1/notify','{}','{}',?,?,now()+interval '1 day',now(),now())",
                task, task, direction, endpoint, revision, status, phase);
        return task;
    }

    private static String id() { return UUID.randomUUID().toString(); }

    @AfterAll static void cleanup(@Autowired org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor scheduler) throws Exception {
        scheduler.destroy();
        if (!SCHEMA.matches("console_it_[a-f0-9]{32}")) throw new IllegalStateException("Unsafe schema");
        try (var connection = DriverManager.getConnection(URL, USER, PASSWORD); var statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE");
        }
    }
}
