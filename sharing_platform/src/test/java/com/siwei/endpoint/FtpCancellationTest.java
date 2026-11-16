package com.siwei.endpoint;

import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import com.siwei.StreamingFtpServer;
import com.siwei.protocol.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;

class FtpCancellationTest {
    private RemoteFileClient client(StreamingFtpServer server, boolean passive) throws IOException {
        EndpointSettings settings = new EndpointSettings(); settings.setMasterKey(Base64.getEncoder().encodeToString(new byte[32]));
        CredentialCipher cipher = new CredentialCipher(settings);
        EndpointRevision r = new EndpointRevision(); r.id = UUID.randomUUID().toString(); r.protocol = EndpointRevision.Protocol.FTP;
        r.host = "127.0.0.1"; r.port = server.port(); r.username = "tester"; r.auth = EndpointRevision.Auth.PASSWORD;
        r.secret = cipher.encrypt("test-only", r.id); r.readRoot = "/"; r.writeRoot = "/";
        r.timeoutSeconds = 5; r.passive = passive; r.isolatedFtpRoot = true;
        return new RemoteFileClientFactory(cipher, settings).connect(r);
    }

    @ParameterizedTest @ValueSource(booleans = {true, false})
    void closeStopsContinuousRetrAndDoesNotCloseCallerTarget(boolean passive) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicLong received = new AtomicLong(); AtomicBoolean targetClosed = new AtomicBoolean();
        try (StreamingFtpServer server = new StreamingFtpServer(StreamingFtpServer.Fault.RETR);
             RemoteFileClient client = client(server, passive)) {
            OutputStream target = new OutputStream() {
                @Override public void write(int b) { received.incrementAndGet(); }
                @Override public void write(byte[] b, int offset, int length) { received.addAndGet(length); }
                @Override public void close() { targetClosed.set(true); }
            };
            Future<Throwable> transfer = executor.submit(() -> failure(() -> client.read("/sample.bin", target)));
            StreamingFtpServer.await(() -> received.get() >= 32768, Duration.ofSeconds(3));
            closeWithinDeadline(client);
            assertThat(transfer.get(2, TimeUnit.SECONDS)).isInstanceOf(IOException.class);
            long finalBytes = received.get();
            StreamingFtpServer.await(() -> server.stopped.get() == 1, Duration.ofSeconds(2));
            assertThat(received.get()).isEqualTo(finalBytes);
            assertThat(targetClosed).isFalse(); client.close();
            assertThat(server.failures).isEmpty();
        } finally { executor.shutdownNow(); assertThat(executor.awaitTermination(3, TimeUnit.SECONDS)).isTrue(); }
    }

    @Test void closeStopsStorWithoutFlushingRemoteBufferOnCancellingThreadOrClosingCallerSource() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicBoolean sourceClosed = new AtomicBoolean();
        try (StreamingFtpServer server = new StreamingFtpServer(StreamingFtpServer.Fault.STOR);
             RemoteFileClient client = client(server, true)) {
            InputStream source = new InputStream() {
                @Override public int read() { return 1; }
                @Override public int read(byte[] b, int offset, int length) { Arrays.fill(b, offset, offset + length, (byte) 1); return length; }
                @Override public void close() { sourceClosed.set(true); }
            };
            Future<Throwable> transfer = executor.submit(() -> failure(() -> client.write("/upload.bin", source)));
            StreamingFtpServer.await(() -> server.bytes.get() >= 32768, Duration.ofSeconds(3));
            closeWithinDeadline(client);
            assertThat(transfer.get(2, TimeUnit.SECONDS)).isInstanceOf(IOException.class);
            assertThat(sourceClosed).isFalse();
        } finally { executor.shutdownNow(); assertThat(executor.awaitTermination(3, TimeUnit.SECONDS)).isTrue(); }
    }

    @ParameterizedTest @ValueSource(booleans = {true, false})
    void closeStopsContinuousDirectoryListing(boolean passive) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (StreamingFtpServer server = new StreamingFtpServer(StreamingFtpServer.Fault.LIST);
             RemoteFileClient client = client(server, passive)) {
            Future<Throwable> listing = executor.submit(() -> failure(() -> client.list("/")));
            StreamingFtpServer.await(() -> server.bytes.get() > 100, Duration.ofSeconds(3));
            closeWithinDeadline(client);
            assertThat(listing.get(2, TimeUnit.SECONDS)).isInstanceOf(IOException.class);
            StreamingFtpServer.await(() -> server.stopped.get() == 1, Duration.ofSeconds(2));
        } finally { executor.shutdownNow(); assertThat(executor.awaitTermination(3, TimeUnit.SECONDS)).isTrue(); }
    }

    @Test void closeDuringPassiveOpeningClosesDataSocketBeforeStreamIsReturned() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (StreamingFtpServer server = new StreamingFtpServer(StreamingFtpServer.Fault.OPENING);
             RemoteFileClient client = client(server, true)) {
            Future<Throwable> opening = executor.submit(() -> failure(() -> client.read("/sample.bin", OutputStream.nullOutputStream())));
            assertThat(server.opening.await(3, TimeUnit.SECONDS)).isTrue();
            closeWithinDeadline(client);
            server.openingDataClosed.get(2, TimeUnit.SECONDS);
            server.releaseOpening.countDown();
            assertThat(opening.get(2, TimeUnit.SECONDS)).isInstanceOf(IOException.class);
        } finally { executor.shutdownNow(); assertThat(executor.awaitTermination(3, TimeUnit.SECONDS)).isTrue(); }
    }

    @Test void closeUnblocksActiveAcceptBeforeIdleTimeout() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (StreamingFtpServer server = new StreamingFtpServer(StreamingFtpServer.Fault.ACTIVE_ACCEPT);
             RemoteFileClient client = client(server, false)) {
            Future<Throwable> opening = executor.submit(() -> failure(() -> client.read("/sample.bin", OutputStream.nullOutputStream())));
            assertThat(server.opening.await(3, TimeUnit.SECONDS)).isTrue();
            closeWithinDeadline(client);
            assertThat(opening.get(2, TimeUnit.SECONDS)).isInstanceOf(IOException.class);
            server.releaseOpening.countDown();
        } finally { executor.shutdownNow(); assertThat(executor.awaitTermination(3, TimeUnit.SECONDS)).isTrue(); }
    }

    private static void closeWithinDeadline(RemoteFileClient client) throws Exception {
        CompletableFuture<Void> closing = CompletableFuture.runAsync(() -> {
            try { client.close(); } catch (IOException e) { throw new UncheckedIOException(e); }
        });
        closing.get(1, TimeUnit.SECONDS);
    }
    private static Throwable failure(IoAction action) {
        try { action.run(); return null; } catch (Exception e) { return e; }
    }
    @FunctionalInterface private interface IoAction { void run() throws IOException; }
}
