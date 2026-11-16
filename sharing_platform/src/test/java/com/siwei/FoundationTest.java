package com.siwei;

import java.util.HashMap;
import java.util.Map;
import com.siwei.config.DatabaseProperties;
import javax.validation.Validation;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FoundationTest {
    @Test
    void databaseTestsRejectUnspecifiedOrUnsafeTargets() {
        for (String url : new String[] {"", "jdbc:h2:mem:test", "jdbc:postgresql://localhost/postgres",
                "jdbc:postgresql://localhost/sharing_platform", "jdbc:postgresql://localhost/isolated_test?currentSchema=public",
                "jdbc:postgresql://user:secret@localhost/isolated_test", "jdbc:postgresql://localhost/isolated_test#fragment"}) {
            assertThatThrownBy(() -> TestDatabase.requireUrl(Map.of("TEST_DB_URL", url)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> TestDatabase.requireUrl(Map.of())).isInstanceOf(IllegalArgumentException.class);
        Map<String, String> environment = new HashMap<>();
        environment.put("TEST_DB_URL", "jdbc:postgresql://localhost/isolated_test");
        assertThat(TestDatabase.requireUrl(environment)).endsWith("/isolated_test");
        environment.put("DB_URL", environment.get("TEST_DB_URL"));
        assertThatThrownBy(() -> TestDatabase.requireUrl(environment)).isInstanceOf(IllegalArgumentException.class);
        environment.put("TEST_DB_ALLOW_LOCAL", "true");
        environment.put("TEST_DB_URL", "jdbc:postgresql://127.0.0.1:5432/sharing_platform");
        environment.put("DB_URL", environment.get("TEST_DB_URL"));
        assertThat(TestDatabase.requireUrl(environment)).isEqualTo(environment.get("DB_URL"));
        environment.put("TEST_DB_URL", "jdbc:postgresql://remote.example/sharing_platform");
        assertThatThrownBy(() -> TestDatabase.requireUrl(environment)).isInstanceOf(IllegalArgumentException.class);
        environment.put("TEST_DB_URL", "jdbc:postgresql://remote.example/sharing_platform_test");
        environment.put("DB_URL", environment.get("TEST_DB_URL"));
        assertThatThrownBy(() -> TestDatabase.requireUrl(environment)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void databaseSettingsRequireExplicitCredentialsAndRejectCredentialsInUrl() {
        try (var factory = Validation.buildDefaultValidatorFactory()) {
            var validator = factory.getValidator();
            for (String url : new String[] {"", "jdbc:h2:mem:test", "jdbc:postgresql://user:secret@localhost/db",
                    "jdbc:postgresql://localhost/db?password=secret", "jdbc:postgresql://localhost/db?USER=admin"}) {
                assertThat(validator.validate(new DatabaseProperties(url, "user", "secret"))).isNotEmpty();
            }
            assertThat(validator.validate(new DatabaseProperties("jdbc:postgresql://localhost/db", "user", ""))).isNotEmpty();
            var configured = new DatabaseProperties("jdbc:postgresql://localhost/db?sslmode=verify-full", "user", "secret");
            assertThat(validator.validate(configured)).isEmpty();
            assertThat(configured.toString()).doesNotContain("secret");
        }
    }
}
