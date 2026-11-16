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
    private boolean pushIntakeEnabled = true;
    private boolean allowZeroByteFiles;
    private Set<String> temporarySuffixes = new HashSet<>(Set.of(".tmp", ".part", ".crdownload"));

    public Map<String, Caller> getCallers() { return callers; }
    public void setCallers(Map<String, Caller> callers) { this.callers = callers; }
    public int getWorkers() { return workers; }
    public void setWorkers(int workers) { this.workers = workers; }
    public int getNotificationAttempts() { return notificationAttempts; }
    public void setNotificationAttempts(int notificationAttempts) { this.notificationAttempts = notificationAttempts; }
    public boolean isProvisionalXmlEnabled() { return provisionalXmlEnabled; }
    public void setProvisionalXmlEnabled(boolean provisionalXmlEnabled) { this.provisionalXmlEnabled = provisionalXmlEnabled; }
    public boolean isPushIntakeEnabled() { return pushIntakeEnabled; }
    public void setPushIntakeEnabled(boolean pushIntakeEnabled) { this.pushIntakeEnabled = pushIntakeEnabled; }
    public boolean isAllowZeroByteFiles() { return allowZeroByteFiles; }
    public void setAllowZeroByteFiles(boolean allowZeroByteFiles) { this.allowZeroByteFiles = allowZeroByteFiles; }
    public Set<String> getTemporarySuffixes() { return temporarySuffixes; }
    public void setTemporarySuffixes(Set<String> temporarySuffixes) { this.temporarySuffixes = temporarySuffixes; }

    public static class Caller {
        private String token;
        private Set<String> endpointIds = new HashSet<>();
        private Map<String, String> xmlRoutes = new HashMap<>();
        private Map<String, Route> routes = new HashMap<>();
        private String notificationUrl;
        public String getToken() { return token; }
        public void setToken(String token) { this.token = token; }
        public Set<String> getEndpointIds() { return endpointIds; }
        public void setEndpointIds(Set<String> endpointIds) { this.endpointIds = endpointIds; }
        public Map<String, String> getXmlRoutes() { return xmlRoutes; }
        public void setXmlRoutes(Map<String, String> xmlRoutes) { this.xmlRoutes = xmlRoutes; }
        public Map<String, Route> getRoutes() { return routes; }
        public void setRoutes(Map<String, Route> routes) { this.routes = routes; }
        public String getNotificationUrl() { return notificationUrl; }
        public void setNotificationUrl(String notificationUrl) { this.notificationUrl = notificationUrl; }
    }

    public static class Route {
        private boolean enabled = true;
        private String endpointId, sourceRoot, targetRoot, notificationUrl, version = "1";
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public String getEndpointId() { return endpointId; }
        public void setEndpointId(String value) { endpointId = value; }
        public String getSourceRoot() { return sourceRoot; }
        public void setSourceRoot(String value) { sourceRoot = value; }
        public String getTargetRoot() { return targetRoot; }
        public void setTargetRoot(String value) { targetRoot = value; }
        public String getNotificationUrl() { return notificationUrl; }
        public void setNotificationUrl(String value) { notificationUrl = value; }
        public String getVersion() { return version; }
        public void setVersion(String value) { version = value; }
    }
}
