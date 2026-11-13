package com.siwei.web;

import java.io.IOException;
import java.util.UUID;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class TraceFilter extends OncePerRequestFilter {
    private static final Logger log = LoggerFactory.getLogger(TraceFilter.class);

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String traceId = UUID.randomUUID().toString();
        request.setAttribute("traceId", traceId);
        response.setHeader("X-Trace-Id", traceId);
        MDC.put("traceId", traceId);
        try {
            chain.doFilter(request, response);
        } finally {
            log.info("request completed method={} status={}", request.getMethod(), response.getStatus());
            MDC.remove("traceId");
        }
    }
}
