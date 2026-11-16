package com.siwei.transfer;

import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class LegacyXmlTest {
    @Test void provisionalNoticeEscapesValuesAndAckRequiresBothIds() {
        Task task = new Task(); task.taskRecordId = "internal"; task.taskId = "external";
        task.products = java.util.List.of(Map.of("product_id", "a", "product_path", "a&b"));
        String xml = LegacyXml.notification("notice", task, "COMPLETE",
                new TaskProcessor.Result("FAILED", "COMPLETE", "PATH", "bad <path>"));
        assertThat(xml).contains("<status>1</status>", "<fileCount>1</fileCount>", "a&amp;b")
                .doesNotContain("a&b");
        assertThat(LegacyXml.ack("<ack><accepted>true</accepted><notificationId>notice</notificationId><taskRecordId>internal</taskRecordId></ack>", "notice", "internal")).isTrue();
        assertThat(LegacyXml.ack("<ack><accepted>true</accepted><notificationId>wrong</notificationId><taskRecordId>internal</taskRecordId></ack>", "notice", "internal")).isFalse();
    }
}
