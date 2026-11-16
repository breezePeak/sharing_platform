package com.siwei.transfer;

import java.util.*;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "platform.transfers")
public class TransferSettings {
    private Map<String, Caller> callers = new HashMap<>();
    private int workers = 2;
    private int notificationAttempts = 5;
    private boolean provisionalXmlEnabled;

    public Map<String, Caller> getCallers() { return callers; }
    public void setCallers(Map<String, Caller> callers) { this.callers = callers; }
    public int getWorkers() { return workers; }
    public void setWorkers(int workers) { this.workers = workers; }
    public int getNotificationAttempts() { return notificationAttempts; }
    public void setNotificationAttempts(int notificationAttempts) { this.notificationAttempts = notificationAttempts; }
    public boolean isProvisionalXmlEnabled() { return provisionalXmlEnabled; }
    public void setProvisionalXmlEnabled(boolean provisionalXmlEnabled) { this.provisionalXmlEnabled = provisionalXmlEnabled; }

    public static class Caller {
        private String token;
        private Set<String> endpointIds = new HashSet<>();
        private Map<String, String> xmlRoutes = new HashMap<>();
        private String notificationUrl;
        public String getToken() { return token; }
        public void setToken(String token) { this.token = token; }
        public Set<String> getEndpointIds() { return endpointIds; }
        public void setEndpointIds(Set<String> endpointIds) { this.endpointIds = endpointIds; }
        public Map<String, String> getXmlRoutes() { return xmlRoutes; }
        public void setXmlRoutes(Map<String, String> xmlRoutes) { this.xmlRoutes = xmlRoutes; }
        public String getNotificationUrl() { return notificationUrl; }
        public void setNotificationUrl(String notificationUrl) { this.notificationUrl = notificationUrl; }
    }
}
