package com.siwei.transfer;

import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.*;

class PushProcessorTest {
    @TempDir Path root;
    private final PushProcessor processor = new PushProcessor(null, null, null, new TransferSettings(), null,
            org.mockito.Mockito.mock(org.springframework.transaction.PlatformTransactionManager.class));

    private Task task(String... paths) {
        Task task = new Task();
        List<Map<String, Object>> products = new ArrayList<>();
        for (int i = 0; i < paths.length; i++) products.add(Map.of("product_id", "p" + i, "product_path", paths[i]));
        task.products = products;
        return task;
    }

    @Test void scansNestedFilesAndRejectsOverlappingProducts() throws Exception {
        Files.createDirectories(root.resolve("a/nested"));
        Files.writeString(root.resolve("a/nested/data.bin"), "original");
        assertThat(processor.scan(root, task("a"))).hasSize(1);
        assertThatThrownBy(() -> processor.scan(root, task("a", "a/nested")))
                .isInstanceOf(java.io.IOException.class).hasMessage("PRODUCT_PATH_OVERLAP");
    }

    @Test void changedContentWithSameSizeCannotMatchFrozenSnapshot() throws Exception {
        Files.createDirectories(root.resolve("a"));
        Path file = root.resolve("a/data.bin");
        Files.writeString(file, "original");
        Task task = task("a");
        var frozen = processor.scan(root, task);
        Files.writeString(file, "replaced");
        assertThatThrownBy(() -> processor.verifySource(root, task, frozen, processor.scanDirectories(root, task)))
                .isInstanceOf(java.io.IOException.class).hasMessage("SOURCE_CHANGED");
    }

    @Test void sourceCollectionChangesAfterFreezeAreRejected() throws Exception {
        Files.createDirectories(root.resolve("a"));
        Files.writeString(root.resolve("a/first.bin"), "first");
        Task task = task("a");
        var files = processor.scan(root, task);
        var dirs = processor.scanDirectories(root, task);
        Files.writeString(root.resolve("a/second.bin"), "second");
        assertThatThrownBy(() -> processor.verifySource(root, task, files, dirs))
                .isInstanceOf(PushProcessor.SourceChanged.class).hasMessage("SOURCE_SET_CHANGED");
    }

    @Test void rejectsEmptyAndTemporaryFiles() throws Exception {
        Files.createDirectories(root.resolve("a"));
        assertThatThrownBy(() -> processor.scan(root, task("a")))
                .isInstanceOf(java.io.IOException.class).hasMessage("EMPTY_PRODUCT");
        Files.write(root.resolve("a/empty.bin"), new byte[0]);
        assertThatThrownBy(() -> processor.scan(root, task("a")))
                .isInstanceOf(java.io.IOException.class).hasMessage("EMPTY_OR_TEMPORARY_FILE");
        Files.delete(root.resolve("a/empty.bin"));
        Files.writeString(root.resolve("a/.sharing-incomplete"), "x");
        assertThatThrownBy(() -> processor.scan(root, task("a")))
                .isInstanceOf(java.io.IOException.class).hasMessage("EMPTY_OR_TEMPORARY_FILE");
    }
}
