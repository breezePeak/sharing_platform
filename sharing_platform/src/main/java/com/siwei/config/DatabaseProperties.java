package com.siwei.config;

import java.net.URI;
import javax.validation.constraints.AssertTrue;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("spring.datasource")
public class DatabaseProperties {
    private String url;
    private String username;
    private String password;

    public DatabaseProperties() { }

    public DatabaseProperties(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }
    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }
    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }
    @AssertTrue(message = "PostgreSQL URL, username and password are required; provide DB_* or TEST_DB_* externally")
    public boolean isConnectionConfigured() {
        return url != null && !url.isBlank() && username != null && !username.isBlank()
                && password != null && !password.isBlank();
    }

    @AssertTrue(message = "Use jdbc:postgresql://host:port/database with credentials configured separately")
    public boolean isUrlAllowed() {
        if (url == null || !url.startsWith("jdbc:postgresql://")) {
            return false;
        }
        try {
            URI parsed = URI.create(url.substring(5));
            String query = parsed.getQuery();
            return parsed.getHost() != null && parsed.getUserInfo() == null && parsed.getFragment() == null
                    && parsed.getPath() != null && parsed.getPath().matches("/[a-zA-Z0-9_-]+")
                    && (query == null || !java.util.regex.Pattern.compile("(?i)(^|&)(user|password|sslpassword|passfile)=")
                            .matcher(query).find());
        } catch (IllegalArgumentException exception) {
            return false;
        }
    }

    @Override
    public String toString() {
        return "DatabaseProperties[redacted]";
    }
}
