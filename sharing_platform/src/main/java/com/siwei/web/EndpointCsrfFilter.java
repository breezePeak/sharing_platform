package com.siwei.web;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.UUID;
import java.util.Base64;
import javax.servlet.*;
import javax.servlet.http.*;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
public class EndpointCsrfFilter extends OncePerRequestFilter {
    public static final String TOKEN = "endpointCsrf";
    public static final String ACTOR = "consoleActor";
    private final byte[] credentials;
    private final String username;
    private final boolean testWithoutCredentials;

    public EndpointCsrfFilter(@Value("${platform.console.username:}") String username,
                             @Value("${platform.console.password:}") String password, Environment environment) {
        this.username = username;
        this.credentials = username.isBlank() || password.isBlank() || username.length() > 100 || username.contains(":")
                ? null : (username + ":" + password).getBytes(StandardCharsets.UTF_8);
        this.testWithoutCredentials = username.isEmpty() && password.isEmpty()
                && environment.acceptsProfiles(org.springframework.core.env.Profiles.of("test"));
    }
    @Override protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String path = org.springframework.web.util.UrlPathHelper.defaultInstance.getPathWithinApplication(request);
        if (path.equals("/services") || path.startsWith("/services/") || path.equals("/config") || path.startsWith("/config/")
                || path.equals("/tasks") || path.startsWith("/tasks/")) {
            response.setHeader("Cache-Control", "no-store");
            if (!testWithoutCredentials) {
                if (credentials == null) { response.sendError(503); return; }
                String authorization = request.getHeader("Authorization");
                if (!authorized(authorization)) {
                    response.setHeader("WWW-Authenticate", "Basic realm=\"Sharing Console\", charset=\"UTF-8\"");
                    response.sendError(401); return;
                }
            }
            request.setAttribute(ACTOR, testWithoutCredentials ? "integration-test" : username);
            if (request.getMethod().equals("GET")) {
                HttpSession session = request.getSession();
                if (session.getAttribute(TOKEN) == null) session.setAttribute(TOKEN, UUID.randomUUID().toString());
            } else if (!request.getMethod().equals("HEAD") && !request.getMethod().equals("OPTIONS")) {
                HttpSession session = request.getSession(false);
                String expected = session == null ? null : (String) session.getAttribute(TOKEN);
                String actual = request.getParameter("_csrf");
                if (expected == null || actual == null || !MessageDigest.isEqual(expected.getBytes(StandardCharsets.UTF_8),
                        actual.getBytes(StandardCharsets.UTF_8))) {
                    response.sendError(403); return;
                }
            }
        }
        chain.doFilter(request, response);
    }

    private boolean authorized(String header) {
        if (header == null || !header.regionMatches(true, 0, "Basic ", 0, 6) || header.length() > 4096) return false;
        try { return MessageDigest.isEqual(credentials, Base64.getDecoder().decode(header.substring(6))); }
        catch (IllegalArgumentException error) { return false; }
    }
}
