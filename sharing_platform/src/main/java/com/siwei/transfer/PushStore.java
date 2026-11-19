package com.siwei.transfer;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class PushStore {
    private final TransferTaskRepository tasks;
    private final TransferFileRepository files;
    private final TransferDirectoryRepository directories;
    public PushStore(TransferTaskRepository tasks, TransferFileRepository files, TransferDirectoryRepository directories) {
        this.tasks = tasks; this.files = files; this.directories = directories;
    }
    public TransferTaskRecord snapshot(String id) { return tasks.findById(id).orElseThrow(); }
    public List<TransferFileRecord> files(String id) {
        List<TransferFileRecord> result = files.findByTaskId(id);
        result.sort(TransferFileRecord.ORDER);
        return result;
    }
    public List<String> directories(String id) {
        return directories.findByTaskId(id).stream().map(d -> d.product + "/" + d.path).sorted().collect(Collectors.toList());
    }
    private TransferTaskRecord executing(Task task) {
        TransferTaskRecord record = tasks.lockById(task.taskRecordId).orElseThrow();
        if (task.claimId == null || !task.claimId.equals(record.claimId) || !"RUNNING".equals(record.status))
            throw new IllegalStateException("EXECUTION_CLAIM_LOST");
        record.updatedAt = Instant.now();
        return record;
    }
    public void freeze(Task task, String source, String target, String digest, String manifest,
                       List<TransferFileRecord> frozen, List<String> dirs) {
        TransferTaskRecord record = executing(task);
        if (!source.equals(record.pushSourceRoot)) throw new IllegalStateException("SOURCE_ROOT_CHANGED");
        if (record.pushManifestSha256 != null) {
            if (!digest.equals(record.pushManifestSha256) || !target.equals(record.pushTargetRoot))
                throw new IllegalStateException("FROZEN_MANIFEST_CHANGED");
            return;
        }
        for (TransferFileRecord file : frozen) file.taskId = task.taskRecordId;
        files.saveAll(frozen);
        for (String dir : dirs) directories.save(new TransferDirectoryRecord(task.taskRecordId, dir));
        record.pushManifestSha256 = digest; record.pushTargetRoot = target; record.pushManifest = manifest;
    }
    public void phase(Task task, String phase) { executing(task).phase = phase; }
    public void uploadStarted(Task task, TransferFileRecord file) {
        TransferTaskRecord record = executing(task);
        record.currentFile = file.product + "/" + file.path;
        files.findById(new TransferPathId(task.taskRecordId, file.product, file.path)).orElseThrow().effectiveUploadedBytes = 0;
    }
    public void manifestIntent(Task task) {
        TransferTaskRecord record = executing(task); record.pushManifestIntent = true; record.phase = "PUBLISHING";
    }
    public void publishIntent(Task task) {
        TransferTaskRecord record = executing(task); record.pushPublishIntent = true; record.phase = "PUBLISHING";
    }
    public void progress(Task task, TransferFileRecord file, long uploaded, long traffic, long readback, boolean verified) {
        TransferTaskRecord record = executing(task);
        TransferFileRecord current = files.findById(new TransferPathId(task.taskRecordId, file.product, file.path)).orElseThrow();
        current.uploadedBytes = Math.max(current.uploadedBytes, Math.min(file.size, uploaded));
        record.currentFile = file.product + "/" + file.path;
        if (traffic > 0) current.effectiveUploadedBytes = Math.min(file.size, uploaded);
        current.uploadTrafficBytes += traffic; current.readbackBytes += readback;
        if (verified) {
            current.verified = true;
            current.effectiveUploadedBytes = current.uploadedBytes = current.size;
            record.attempts = 1;
        }
    }
}
