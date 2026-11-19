package com.siwei.endpoint;

import com.siwei.ProtocolServers;
import com.siwei.protocol.RemoteFileClientFactory;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.security.*;
import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class ReleaseCapacityIT {
    @TempDir Path root;

    @Test void ftpStreamsAndVerifies256MiB() throws Exception {
        long size = 256L * 1024 * 1024;
        Path file = root.resolve("large.bin");
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            channel.position(size - 1);
            channel.write(java.nio.ByteBuffer.wrap(new byte[1]));
        }
        var expected = MessageDigest.getInstance("SHA-256");
        try (var input = new DigestInputStream(Files.newInputStream(file), expected)) { assertThat(input.transferTo(OutputStream.nullOutputStream())).isEqualTo(size); }
        try (var protocol = new ProtocolServers(root, EndpointRevision.Protocol.FTP, ProtocolServers.Mode.NORMAL);
             var measurement = new Measurement("FTP", 1, size)) {
            var endpoint = endpoint(protocol, EndpointRevision.Protocol.FTP);
            try (var remote = endpoint.factory.connect(endpoint.revision)) {
                var actual = MessageDigest.getInstance("SHA-256");
                try (var sink = new DigestOutputStream(OutputStream.nullOutputStream(), actual)) { remote.read("/large.bin", sink); }
                assertThat(remote.lstat("/large.bin").size).isEqualTo(size);
                assertThat(actual.digest()).isEqualTo(expected.digest());
            } finally { endpoint.factory.close(); }
            measurement.passed = 1;
        }
    }

    @Test void sftpListsAndReads1000SmallFiles() throws Exception {
        int files = 1000, size = 4096;
        Path directory = Files.createDirectory(root.resolve("bulk"));
        byte[] content = new byte[size];
        for (int index = 0; index < files; index++) {
            java.nio.ByteBuffer.wrap(content).putInt(index);
            Files.write(directory.resolve("file-" + index + ".bin"), content);
        }
        try (var protocol = new ProtocolServers(root, EndpointRevision.Protocol.SFTP, ProtocolServers.Mode.NORMAL);
             var measurement = new Measurement("SFTP", files, (long) size * files)) {
            var endpoint = endpoint(protocol, EndpointRevision.Protocol.SFTP);
            try (var remote = endpoint.factory.connect(endpoint.revision)) {
                assertThat(remote.list("/bulk")).hasSize(files);
                for (int index = 0; index < files; index++) {
                    java.nio.ByteBuffer.wrap(content).putInt(index);
                    var sink = new java.io.ByteArrayOutputStream(size);
                    remote.read("/bulk/file-" + index + ".bin", sink);
                    assertThat(sink.toByteArray()).isEqualTo(content);
                    measurement.passed++;
                }
            } finally { endpoint.factory.close(); }
        }
    }

    private static Endpoint endpoint(ProtocolServers server, EndpointRevision.Protocol protocol) {
        var settings = new EndpointSettings();
        byte[] key = new byte[32];
        new SecureRandom().nextBytes(key);
        settings.setMasterKey(Base64.getEncoder().encodeToString(key));
        var cipher = new CredentialCipher(settings);
        EndpointRevision revision = new EndpointRevision();
        revision.id = UUID.randomUUID().toString();
        revision.protocol = protocol; revision.host = "127.0.0.1"; revision.port = server.port;
        revision.username = "tester"; revision.auth = EndpointRevision.Auth.PASSWORD;
        revision.secret = cipher.encrypt(ProtocolServers.PASSWORD, revision.id);
        revision.fingerprint = server.fingerprint; revision.readRoot = "/";
        revision.timeoutSeconds = 15; revision.passive = true; revision.isolatedFtpRoot = true;
        return new Endpoint(new RemoteFileClientFactory(cipher, settings), revision);
    }

    private static final class Endpoint {
        final RemoteFileClientFactory factory;
        final EndpointRevision revision;
        Endpoint(RemoteFileClientFactory factory, EndpointRevision revision) { this.factory = factory; this.revision = revision; }
    }

    private static final class Measurement implements AutoCloseable {
        final String protocol;
        final int files;
        final long bytes, started = System.nanoTime();
        final AtomicLong heap = new AtomicLong(), threads = new AtomicLong();
        final ScheduledExecutorService samples = Executors.newSingleThreadScheduledExecutor();
        int passed;

        Measurement(String protocol, int files, long bytes) {
            this.protocol = protocol; this.files = files; this.bytes = bytes;
            samples.scheduleAtFixedRate(() -> {
                heap.accumulateAndGet(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed(), Math::max);
                threads.accumulateAndGet(ManagementFactory.getThreadMXBean().getThreadCount(), Math::max);
            }, 0, 100, TimeUnit.MILLISECONDS);
        }

        @Override public void close() {
            samples.shutdownNow();
            double elapsed = (System.nanoTime() - started) / 1_000_000_000.0;
            System.out.printf(java.util.Locale.ROOT, "RELEASE_LOAD protocol=%s files=%d bytes=%d seconds=%.3f MiB_per_second=%.2f heap_peak_bytes=%d threads_peak=%d configured_sessions=1 failed=%d%n",
                    protocol, files, bytes, elapsed, bytes / (1024.0 * 1024 * elapsed), heap.get(), threads.get(), files - passed);
        }
    }
}
