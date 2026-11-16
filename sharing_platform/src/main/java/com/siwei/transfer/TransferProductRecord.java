package com.siwei.transfer;

import java.io.Serializable;
import javax.persistence.*;

@Entity
@Table(name = "transfer_product")
@IdClass(TransferProductRecord.Key.class)
public class TransferProductRecord {
    @Id @Column(name = "task_id") public String taskId;
    @Id public int position;
    @Column(name = "product_id") public String productId;
    @Column(name = "product_path") public String productPath;
    @Column(name = "product_name") public String productName;

    public static class Key implements Serializable {
        public String taskId;
        public int position;
        public Key() { }
        @Override public boolean equals(Object other) {
            if (!(other instanceof Key)) return false;
            Key key = (Key) other;
            return position == key.position && java.util.Objects.equals(taskId, key.taskId);
        }
        @Override public int hashCode() { return java.util.Objects.hash(taskId, position); }
    }
}
