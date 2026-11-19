package com.siwei;

import java.sql.DriverManager;
import java.util.Map;
import java.util.UUID;
import com.siwei.metadata.MetadataRepository;
import com.siwei.metadata.PlatformMetadata;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@ActiveProfiles("test")
@AutoConfigureMockMvc
@DirtiesContext
class PostgresIT {
    private static final Map<String, String> ENV = System.getenv();
    private static final String URL = TestDatabase.requireUrl(ENV);
    private static final String USER = TestDatabase.require(ENV, "TEST_DB_USERNAME");
    private static final String PASSWORD = TestDatabase.require(ENV, "TEST_DB_PASSWORD");
    private static final String SCHEMA = "foundation_it_" + UUID.randomUUID().toString().replace("-", "");

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry properties) {
        properties.add("spring.datasource.url", () -> URL);
        properties.add("spring.datasource.username", () -> USER);
        properties.add("spring.datasource.password", () -> PASSWORD);
        properties.add("spring.datasource.hikari.schema", () -> SCHEMA);
        properties.add("spring.flyway.default-schema", () -> SCHEMA);
    }

    @Autowired MetadataRepository repository;
    @Autowired Flyway flyway;
    @Autowired MockMvc mvc;

    @Test
    void emptyDatabaseMigratesAndRepositoryReadsAndWritesRealPostgres() throws Exception {
        assertThat(repository.findById("platform").map(PlatformMetadata::getValue)).contains("sharing-platform");
        repository.saveAndFlush(new PlatformMetadata("repository-test", "真实数据库记录"));
        assertThat(repository.findById("repository-test").map(PlatformMetadata::getValue)).contains("真实数据库记录");
        assertThat(repository.findById("missing")).isEmpty();
        try (var connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            assertThat(connection.getMetaData().getDatabaseMajorVersion()).isEqualTo(13);
        }
    }

    @Test
    void repeatedMigrationDoesNotDestroyRecords() {
        repository.saveAndFlush(new PlatformMetadata("repeat-test", "retained"));
        assertThat(flyway.migrate().migrationsExecuted).isZero();
        assertThat(repository.findById("repeat-test").map(PlatformMetadata::getValue)).contains("retained");
    }

    @Test
    void dataSurvivesApplicationRestart() {
        String[] arguments = {
                "--server.port=0", "--spring.datasource.url=" + URL,
                "--spring.datasource.username=" + USER, "--spring.datasource.password=" + PASSWORD,
                "--spring.datasource.hikari.schema=" + SCHEMA, "--spring.flyway.default-schema=" + SCHEMA};
        try (var initial = new SpringApplicationBuilder(App.class).profiles("test").run(arguments)) {
            initial.getBean(MetadataRepository.class).saveAndFlush(new PlatformMetadata("restart-test", "retained-after-restart"));
        }
        try (var restarted = new SpringApplicationBuilder(App.class).profiles("test").run(arguments)) {
            assertThat(restarted.getBean(MetadataRepository.class).findById("restart-test").map(PlatformMetadata::getValue))
                    .contains("retained-after-restart");
        }
    }

    @Test void v4FrozenPushSurvivesUpgradeWithoutRefreezingOrLosingProgress() throws Exception {
        String schema = SCHEMA + "_push_upgrade";
        try {
            Flyway.configure().dataSource(URL, USER, PASSWORD).defaultSchema(schema)
                    .locations("classpath:db/migration").target("4").load().migrate();
            // Migration fixtures intentionally use independent SQL against the V4 schema, before JPA's new columns exist.
            try (var connection = DriverManager.getConnection(URL, USER, PASSWORD); var sql = connection.createStatement()) {
                sql.execute("SET search_path TO " + schema);
                sql.execute("INSERT INTO endpoint(id,revision,enabled) VALUES ('upgrade-endpoint',1,true)");
                sql.execute("INSERT INTO endpoint_revision(id,endpoint_id,revision,name,protocol,host,port,username,auth,timeout_seconds,concurrency,passive,isolated_ftp_root,created_at) VALUES ('upgrade-revision','upgrade-endpoint',1,'upgrade','FTP','127.0.0.1',21,'test','PASSWORD',1,1,true,true,now())");
                sql.execute("INSERT INTO transfer_task(id,caller_id,external_task_id,entry_type,direction,endpoint_id,endpoint_revision_id,notification_url,canonical_request,raw_request,status,phase,next_run_at,created_at,updated_at,push_manifest_sha256,push_source_root,push_target_root) VALUES ('upgrade-task','test','upgrade','JSON','PUSH','upgrade-endpoint','upgrade-revision','http://127.0.0.1/notify','{}','{}','QUEUED','RETRY_WAIT',now(),now(),now(),repeat('a',64),'/source','/target')");
                sql.execute("INSERT INTO transfer_file(task_id,product_id,relative_path,size_bytes,sha256,source_modified_millis,uploaded_bytes,verified) VALUES ('upgrade-task','p','a.bin',7,repeat('b',64),1234,7,true)");
            }
            assertThat(Flyway.configure().dataSource(URL, USER, PASSWORD).defaultSchema(schema)
                    .locations("classpath:db/migration").load().migrate().migrationsExecuted).isEqualTo(3);
            try (var connection = DriverManager.getConnection(URL, USER, PASSWORD); var sql = connection.createStatement();
                 var row = sql.executeQuery("SELECT t.push_manifest_sha256,t.push_target_root,t.push_manifest,t.push_manifest_intent,f.uploaded_bytes,f.verified,f.upload_traffic_bytes,f.readback_bytes FROM "
                         + schema + ".transfer_task t JOIN " + schema + ".transfer_file f ON f.task_id=t.id WHERE t.id='upgrade-task'")) {
                assertThat(row.next()).isTrue(); assertThat(row.getString(1)).isEqualTo("a".repeat(64));
                assertThat(row.getString(2)).isEqualTo("/target"); assertThat(row.getString(3)).isNull();
                assertThat(row.getBoolean(4)).isTrue(); assertThat(row.getLong(5)).isEqualTo(7); assertThat(row.getBoolean(6)).isTrue();
                assertThat(row.getLong(7)).isZero(); assertThat(row.getLong(8)).isZero();
            }
        } finally { dropSchema(schema); }
    }

    @Test
    void olderSchemaUpgradesWithoutLosingDataAndInvalidMigrationFails() throws Exception {
        String upgradeSchema = SCHEMA + "_upgrade";
        try {
            Flyway original = Flyway.configure().dataSource(URL, USER, PASSWORD)
                    .defaultSchema(upgradeSchema).locations("classpath:db/migration").target("1").load();
            assertThat(original.migrate().migrationsExecuted).isEqualTo(1);
            Flyway upgraded = Flyway.configure().dataSource(URL, USER, PASSWORD).defaultSchema(upgradeSchema)
                    .locations("classpath:db/migration", "classpath:db/upgrade").load();
            assertThat(upgraded.migrate().migrationsExecuted).isEqualTo(7);
            try (var connection = DriverManager.getConnection(URL, USER, PASSWORD);
                 var statement = connection.createStatement()) {
                try (var result = statement.executeQuery("SELECT value, description FROM " + upgradeSchema
                        + ".platform_metadata WHERE name = 'platform'")) {
                    assertThat(result.next()).isTrue();
                    assertThat(result.getString("value")).isEqualTo("sharing-platform");
                }
                statement.execute("UPDATE " + upgradeSchema + ".flyway_schema_history SET checksum = 0 WHERE version = '1'");
            }
            assertThatThrownBy(upgraded::validate).isInstanceOf(org.flywaydb.core.api.FlywayException.class);
        } finally {
            dropSchema(upgradeSchema);
        }
    }

    @Test
    void publicHealthOnlyExposesStatusAndOtherActuatorEndpointsAreUnavailable() throws Exception {
        mvc.perform(get("/actuator/health")).andExpect(status().isOk()).andExpect(content().json("{\"status\":\"UP\"}", true));
        mvc.perform(get("/actuator/env")).andExpect(status().isNotFound());
    }

    @AfterAll
    static void cleanup() throws Exception {
        dropSchema(SCHEMA);
    }

    private static void dropSchema(String schema) throws Exception {
        if (!schema.matches("foundation_it_[a-f0-9]{32}(_(?:push_)?upgrade)?")) {
            throw new IllegalArgumentException("Refusing to clean a non-test schema");
        }
        try (var connection = DriverManager.getConnection(URL, USER, PASSWORD);
             var statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
        }
    }
}
