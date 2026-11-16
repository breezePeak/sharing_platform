package com.siwei.endpoint;

import com.siwei.StreamingFtpServer;
import com.siwei.protocol.*;
import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class BusinessDeadlineTest {
    static EndpointRevision revision(CredentialCipher cipher, int port) {
        EndpointRevision r = new EndpointRevision(); r.id = UUID.randomUUID().toString();
        r.protocol = EndpointRevision.Protocol.FTP; r.host = "127.0.0.1"; r.port = port;
        r.auth = EndpointRevision.Auth.PASSWORD; r.username = "tester";
        r.secret = cipher.encrypt("test-only", r.id); r.timeoutSeconds = 1;
        r.passive = true; r.isolatedFtpRoot = true; r.writeRoot = "/"; r.readRoot = "/";
        return r;
    }
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void stalledUploadClosesSocketsAndReleasesTheExecutionThread(boolean shutdown) throws Exception {
        EndpointSettings settings = new EndpointSettings(); settings.setMasterKey(Base64.getEncoder().encodeToString(new byte[32]));
        CredentialCipher cipher = new CredentialCipher(settings);
        RemoteFileClientFactory factory = new RemoteFileClientFactory(cipher, settings);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try (StreamingFtpServer server = new StreamingFtpServer(StreamingFtpServer.Fault.STALLED_STOR)) {
            Future<?> upload = worker.submit(() -> {
                try (RemoteFileClient client = factory.connect(revision(cipher, server.port()))) {
                    client.write("/large.bin", new InputStream() {
                        @Override public int read() { return 1; }
                        @Override public int read(byte[] bytes, int offset, int length) { Arrays.fill(bytes, offset, offset + length, (byte) 1); return length; }
                    });
                    throw new AssertionError("Stalled upload completed");
                } catch (IOException expected) { }
            });
            StreamingFtpServer.await(() -> server.started.get() == 1, Duration.ofSeconds(2));
            if (shutdown) factory.close();
            upload.get(5, TimeUnit.SECONDS);
            StreamingFtpServer.await(() -> server.stopped.get() == 1, Duration.ofSeconds(2));
            assertThat(server.bytes.get()).isZero();
            if (!shutdown) assertThat(worker.submit(() -> {
                try (RemoteFileClient client = factory.connect(revision(cipher, server.port()))) { return client.lstat("/").directory; }
            }).get(3, TimeUnit.SECONDS)).isTrue();
        } finally { worker.shutdownNow(); factory.close(); }
    }

    @Test void continuouslyProgressingUploadIsNotSubjectToATotalDeadline() throws Exception {
        EndpointSettings settings = new EndpointSettings(); settings.setMasterKey(Base64.getEncoder().encodeToString(new byte[32]));
        CredentialCipher cipher = new CredentialCipher(settings);
        RemoteFileClientFactory factory = new RemoteFileClientFactory(cipher, settings);
        try (StreamingFtpServer server = new StreamingFtpServer(StreamingFtpServer.Fault.STOR);
             RemoteFileClient client = factory.connect(revision(cipher, server.port()))) {
            long start = System.nanoTime();
            client.write("/large.bin", new InputStream() {
                int chunks = 60;
                @Override public int read() { throw new UnsupportedOperationException(); }
                @Override public int read(byte[] bytes, int offset, int length) throws IOException {
                    if (chunks-- == 0) return -1;
                    try { Thread.sleep(50); } catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new IOException(error); }
                    Arrays.fill(bytes, offset, offset + length, (byte) 1); return length;
                }
            });
            assertThat(System.nanoTime() - start).isGreaterThan(TimeUnit.SECONDS.toNanos(2));
            assertThat(server.bytes.get()).isGreaterThan(0);
        } finally { factory.close(); }
    }

    @Test void continuousReadbackOutlivesTheInactivityLimit() throws Exception {
        EndpointSettings settings = new EndpointSettings(); settings.setMasterKey(Base64.getEncoder().encodeToString(new byte[32]));
        CredentialCipher cipher = new CredentialCipher(settings);
        RemoteFileClientFactory factory = new RemoteFileClientFactory(cipher, settings);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try (StreamingFtpServer server = new StreamingFtpServer(StreamingFtpServer.Fault.RETR);
             RemoteFileClient client = factory.connect(revision(cipher, server.port()))) {
            Future<?> read = worker.submit(() -> {
                try { client.read("/sample.bin", OutputStream.nullOutputStream()); } catch (IOException closed) { }
            });
            StreamingFtpServer.await(() -> server.bytes.get() > 1024 * 1024, Duration.ofSeconds(6));
            assertThat(read.isDone()).isFalse();
            client.close(); read.get(2, TimeUnit.SECONDS);
        } finally { worker.shutdownNow(); factory.close(); }
    }
}
