package com.siwei.transfer;

import java.util.*;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "platform.pulls")
public class PullSettings {
    private boolean intakeEnabled = true;
    private Map<String, Source> sources = new HashMap<>();
    public boolean isIntakeEnabled() { return intakeEnabled; }
    public void setIntakeEnabled(boolean value) { intakeEnabled = value; }
    public Map<String, Source> getSources() { return sources; }
    public void setSources(Map<String, Source> value) { sources = value; }

    public String snapshot(com.fasterxml.jackson.databind.ObjectMapper json, com.fasterxml.jackson.databind.JsonNode request,
                           com.siwei.endpoint.EndpointRevision endpoint, String caller,
                           com.siwei.endpoint.EndpointSettings roots, TransferSettings transfers) {
        try {
            String id = TaskService.required(request, "sourceId", 100);
            Source source = sources.get(id);
            if (source == null || !source.callerIds.contains(caller))
                throw TaskService.error(org.springframework.http.HttpStatus.FORBIDDEN, "SOURCE_FORBIDDEN");
            if (!endpoint.endpointId.equals(source.endpointId) || !Objects.equals(source.targetRef, endpoint.localTargetRef)
                    || !source.targetRef.equals(TaskService.required(request, "targetPath", 100))) throw new IllegalArgumentException();
            String path = TaskService.required(request, "sourcePath", 1000);
            com.siwei.protocol.ControlledPaths.relative(source.sourcePath);
            com.siwei.protocol.ControlledPaths.relative(path);
            if (!path.equals(source.sourcePath) && !path.startsWith(source.sourcePath + "/")) throw new IllegalArgumentException();
            if (!Set.of("FILE", "PACKAGE_DIR").contains(source.unitType) || !Set.of("BASIC", "HASH").contains(source.validation)
                    || source.scanDepth < 0 || source.scanDepth > 8 || source.stableObservations < 1
                    || source.observationSeconds < 1 || source.acceptanceSeconds < 1) throw new IllegalArgumentException();
            java.nio.file.FileSystems.getDefault().getPathMatcher("glob:" + source.filePattern);
            for (String control : Arrays.asList(source.manifestName, source.readyName))
                if (control != null && (!control.matches("[A-Za-z_][A-Za-z0-9._-]{0,99}") || control.startsWith(".sharing-"))) throw new IllegalArgumentException();
            if (source.manifestName != null && source.manifestName.equals(source.readyName)) throw new IllegalArgumentException();
            java.net.URI access = java.net.URI.create(source.accessBase);
            if (!Set.of("https", "http", "smb").contains(access.getScheme()) || access.getHost() == null
                    || access.getUserInfo() != null || access.getQuery() != null || access.getFragment() != null
                    || access.getRawPath().contains("..")) throw new IllegalArgumentException();
            TransferSettings.Caller recipient = transfers.getCallers().get(source.recipientId);
            if (recipient == null || recipient.getToken() == null || recipient.getToken().isBlank()
                    || !recipient.getEndpointIds().contains(endpoint.endpointId)) throw new IllegalArgumentException();
            java.net.URI url = java.net.URI.create(recipient.getNotificationUrl());
            if (!Set.of("http", "https").contains(url.getScheme()) || url.getHost() == null || url.getUserInfo() != null
                    || url.getFragment() != null) throw new IllegalArgumentException();
            var config = json.valueToTree(source);
            var result = (com.fasterxml.jackson.databind.node.ObjectNode) config;
            result.put("sourceId", id);
            result.put("accessBase", access.normalize().toASCIIString().replaceAll("/+$", ""));
            result.put("localRoot", com.siwei.protocol.ControlledPaths.localDirectory(roots.getLocalRoots().get(source.targetRef)).toString());
            result.put("remoteRoot", endpoint.readRoot);
            result.put("notificationUrl", url.toString());
            return result.toString();
        } catch (java.io.IOException | IllegalArgumentException | NullPointerException error) {
            throw TaskService.error(org.springframework.http.HttpStatus.BAD_REQUEST, "INVALID_PULL_CONFIG");
        }
    }

    public static class Source {
        private String endpointId, sourcePath, targetRef, recipientId, accessBase;
        private String unitType = "FILE", validation = "BASIC", filePattern = "*", manifestName, readyName;
        private int scanDepth, stableObservations = 3, observationSeconds = 60, acceptanceSeconds = 86400;
        private Set<String> callerIds = new HashSet<>();
        public String getEndpointId() { return endpointId; }
        public void setEndpointId(String v) { endpointId = v; }
        public String getSourcePath() { return sourcePath; }
        public void setSourcePath(String v) { sourcePath = v; }
        public String getTargetRef() { return targetRef; }
        public void setTargetRef(String v) { targetRef = v; }
        public String getRecipientId() { return recipientId; }
        public void setRecipientId(String v) { recipientId = v; }
        public String getAccessBase() { return accessBase; }
        public void setAccessBase(String v) { accessBase = v; }
        public String getUnitType() { return unitType; }
        public void setUnitType(String v) { unitType = v; }
        public String getValidation() { return validation; }
        public void setValidation(String v) { validation = v; }
        public String getFilePattern() { return filePattern; }
        public void setFilePattern(String v) { filePattern = v; }
        public String getManifestName() { return manifestName; }
        public void setManifestName(String v) { manifestName = v; }
        public String getReadyName() { return readyName; }
        public void setReadyName(String v) { readyName = v; }
        public int getScanDepth() { return scanDepth; }
        public void setScanDepth(int v) { scanDepth = v; }
        public int getStableObservations() { return stableObservations; }
        public void setStableObservations(int v) { stableObservations = v; }
        public int getObservationSeconds() { return observationSeconds; }
        public void setObservationSeconds(int v) { observationSeconds = v; }
        public int getAcceptanceSeconds() { return acceptanceSeconds; }
        public void setAcceptanceSeconds(int v) { acceptanceSeconds = v; }
        public Set<String> getCallerIds() { return callerIds; }
        public void setCallerIds(Set<String> v) { callerIds = v; }
    }
}
