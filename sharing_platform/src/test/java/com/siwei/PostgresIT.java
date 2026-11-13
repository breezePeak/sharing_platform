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

    @Test
    void olderSchemaUpgradesWithoutLosingDataAndInvalidMigrationFails() throws Exception {
        String upgradeSchema = SCHEMA + "_upgrade";
        try {
            Flyway original = Flyway.configure().dataSource(URL, USER, PASSWORD)
                    .defaultSchema(upgradeSchema).locations("classpath:db/migration").load();
            assertThat(original.migrate().migrationsExecuted).isEqualTo(1);
            Flyway upgraded = Flyway.configure().dataSource(URL, USER, PASSWORD).defaultSchema(upgradeSchema)
                    .locations("classpath:db/migration", "classpath:db/upgrade").load();
            assertThat(upgraded.migrate().migrationsExecuted).isEqualTo(1);
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
        if (!schema.matches("foundation_it_[a-f0-9]{32}(_upgrade)?")) {
            throw new IllegalArgumentException("Refusing to clean a non-test schema");
        }
        try (var connection = DriverManager.getConnection(URL, USER, PASSWORD);
             var statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
        }
    }
}
