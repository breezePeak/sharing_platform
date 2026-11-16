package com.siwei.transfer;

import javax.persistence.*;

@Entity
@Table(name = "transfer_directory")
@IdClass(TransferPathId.class)
public class TransferDirectoryRecord {
    @Id @Column(name = "task_id") public String taskId;
    @Id @Column(name = "product_id") public String product;
    @Id @Column(name = "relative_path") public String path;
    public TransferDirectoryRecord() { }
    TransferDirectoryRecord(String taskId, String directory) {
        this.taskId = taskId;
        int slash = directory.indexOf('/');
        product = directory.substring(0, slash); path = directory.substring(slash + 1);
    }
}
