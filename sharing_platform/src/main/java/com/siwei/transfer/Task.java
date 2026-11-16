package com.siwei.transfer;

import java.time.Instant;
import java.util.*;

public class Task {
    public String taskRecordId, callerId, taskId, entryType, direction, endpointId, endpointRevisionId;
    public String canonicalRequest, executionStatus, phase, acceptanceStatus, errorType, errorDetail;
    public Instant createdAt, updatedAt;
    public Long totalFiles, totalBytes;
    public List<Map<String, Object>> products = Collections.emptyList();
    public List<Map<String, Object>> notifications = Collections.emptyList();
}
