package com.siwei.transfer;

import java.time.Instant;
import java.util.*;

public class Task {
    @com.fasterxml.jackson.annotation.JsonIgnore public String claimId;
    public String taskRecordId, callerId, taskId, entryType, direction, endpointId, endpointRevisionId;
    public String canonicalRequest, executionStatus, phase, acceptanceStatus, errorType, errorDetail;
    public String routeVersion;
    public Instant createdAt, updatedAt;
    public Long totalFiles, totalBytes;
    public Long uploadedBytes, verifiedFiles, verifiedBytes;
    public Long uploadTrafficBytes, readbackBytes, retransmittedBytes;
    public Long downloadedBytes;
    public List<Map<String, Object>> deliveries = Collections.emptyList();
    public List<Map<String, Object>> products = Collections.emptyList();
    public List<Map<String, Object>> notifications = Collections.emptyList();

    public String deliveryPath(Map<String, Object> product) {
        if (!"PUSH".equals(direction)) return (String) product.get("product_path");
        try {
            var request = new com.fasterxml.jackson.databind.ObjectMapper().readTree(canonicalRequest);
            String sourceCode = request.has("sourceCode") ? request.get("sourceCode").asText() : callerId;
            return request.get("targetPath").asText() + "/" + sourceCode + "/" + taskRecordId + "/" + product.get("product_id");
        } catch (com.fasterxml.jackson.core.JsonProcessingException error) { throw new IllegalStateException("INVALID_CANONICAL_REQUEST", error); }
    }
}
