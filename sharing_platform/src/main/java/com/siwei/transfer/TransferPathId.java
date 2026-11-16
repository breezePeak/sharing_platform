package com.siwei.transfer;

import java.io.Serializable;
import java.util.Objects;

public class TransferPathId implements Serializable {
    public String taskId, product, path;
    public TransferPathId() { }
    public TransferPathId(String taskId, String product, String path) {
        this.taskId = taskId; this.product = product; this.path = path;
    }
    @Override public boolean equals(Object other) {
        if (!(other instanceof TransferPathId)) return false;
        TransferPathId id = (TransferPathId) other;
        return Objects.equals(taskId, id.taskId) && Objects.equals(product, id.product) && Objects.equals(path, id.path);
    }
    @Override public int hashCode() { return Objects.hash(taskId, product, path); }
}
