package com.siwei.transfer;

import java.time.Instant;
import java.util.Comparator;
import javax.persistence.*;

@Entity
@Table(name = "transfer_file")
@IdClass(TransferPathId.class)
public class TransferFileRecord {
    public static final Comparator<TransferFileRecord> ORDER = Comparator.comparing((TransferFileRecord f) -> f.product).thenComparing(f -> f.path);
    @Id @Column(name = "task_id") public String taskId;
    @Id @Column(name = "product_id") public String product;
    @Id @Column(name = "relative_path") public String path;
    @Column(name = "size_bytes") public long size;
    @Column(name = "sha256") public String sha;
    @Column(name = "source_modified_millis") public long modifiedMillis;
    @Column(name = "uploaded_bytes") public long uploadedBytes;
    @Column(name = "upload_traffic_bytes") public long uploadTrafficBytes;
    @Column(name = "readback_bytes") public long readbackBytes;
    public boolean verified;
    public TransferFileRecord() { }
    TransferFileRecord(String product, String path, long size, String sha, Instant modified) {
        this.product = product; this.path = path; this.size = size; this.sha = sha; this.modifiedMillis = modified.toEpochMilli();
    }
    boolean same(TransferFileRecord other) {
        return product.equals(other.product) && path.equals(other.path) && size == other.size
                && sha.equals(other.sha) && modifiedMillis == other.modifiedMillis;
    }
}
