package com.siwei.web;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.UUID;
import javax.servlet.*;
import javax.servlet.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
public class EndpointCsrfFilter extends OncePerRequestFilter {
    public static final String TOKEN = "endpointCsrf";
    @Override protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String path = org.springframework.web.util.UrlPathHelper.defaultInstance.getPathWithinApplication(request);
        if (path.startsWith("/config/endpoints") || path.startsWith("/config/listeners")) {
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
}
