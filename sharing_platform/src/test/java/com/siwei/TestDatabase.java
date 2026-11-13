package com.siwei;

import java.net.URI;
import java.util.Map;

final class TestDatabase {
    private TestDatabase() { }

    static String requireUrl(Map<String, String> environment) {
        String url = environment.get("TEST_DB_URL");
        if (url == null || !url.startsWith("jdbc:postgresql://")) {
            throw new IllegalArgumentException("TEST_DB_URL must explicitly name an isolated PostgreSQL test database");
        }
        URI target;
        try {
            target = URI.create(url.substring(5));
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("TEST_DB_URL is invalid");
        }
        if (target.getHost() == null || target.getUserInfo() != null || target.getQuery() != null
                || target.getFragment() != null || target.getPath() == null
                || !target.getPath().matches("/[a-zA-Z0-9_]+_test")) {
            throw new IllegalArgumentException("TEST_DB_URL must name a database ending in _test, without credentials or options");
        }
        if (url.equals(environment.get("DB_URL"))) {
            throw new IllegalArgumentException("The test database must differ from DB_URL");
        }
        return url;
    }

    static String require(Map<String, String> environment, String name) {
        String value = environment.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required for database integration tests");
        }
        return value;
    }
}
