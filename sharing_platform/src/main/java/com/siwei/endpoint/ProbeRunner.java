package com.siwei.endpoint;

import java.io.*;
import java.net.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.PreDestroy;
import com.siwei.protocol.*;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
@org.springframework.context.annotation.DependsOn("startupRecovery")
public class ProbeRunner {
    private final ProbeStore store;
    private final EndpointRevisionRepository revisions;
    private final RemoteFileClientFactory factory;
    private final ExecutorService workers = new ThreadPoolExecutor(2, 2, 0, TimeUnit.SECONDS,
            new SynchronousQueue<>(), r -> daemon(r, "endpoint-probe"), new ThreadPoolExecutor.AbortPolicy());
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> daemon(r, "probe-deadline"));

    public ProbeRunner(ProbeStore store, EndpointRevisionRepository revisions, RemoteFileClientFactory factory) {
        this.store = store; this.revisions = revisions; this.factory = factory;
    }
    private static Thread daemon(Runnable r, String name) { Thread thread = new Thread(r, name); thread.setDaemon(true); return thread; }

    public String start(String endpointId, long revision) {
        EndpointProbe p = store.create(endpointId, revision);
        EndpointRevision r = revisions.findById(p.revisionId).orElseThrow();
        AtomicReference<RemoteFileClient> active = new AtomicReference<>();
        AtomicBoolean cancelled = new AtomicBoolean();
        try {
            workers.execute(() -> {
                ScheduledFuture<?> deadline = timer.schedule(() -> {
                    try {
                        cancelled.set(true);
                        RemoteFileClient client = active.get();
                        if (client != null) try { client.close(); } catch (IOException ignored) { /* residuals are persisted before writes */ }
                    } finally { store.stop(p.id, "TIMED_OUT"); }
                }, r.timeoutSeconds * 6L, TimeUnit.SECONDS);
                try { run(p, r, active, cancelled); }
                finally { deadline.cancel(false); }
            });
        } catch (RejectedExecutionException e) {
            store.stop(p.id, "BUSY");
            throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS);
        }
        return p.id;
    }

    private void run(EndpointProbe p, EndpointRevision r, AtomicReference<RemoteFileClient> active, AtomicBoolean cancelled) {
        try (RemoteFileClient client = factory.connect(r, created -> {
            active.set(created);
            if (cancelled.get()) try { created.close(); } catch (IOException ignored) { }
        })) {
            if (cancelled.get()) throw new Stopped();
            new Execution(p, r, client).probe();
            if (cancelled.get()) return;
            p.status = "COMPLETED";
        } catch (Stopped e) { return;
        } catch (IOException | RuntimeException e) {
            if (cancelled.get()) return;
            p.status = "FAILED"; p.details += "\nCONNECT_OR_SESSION: " + reason(e);
        } finally { active.set(null); }
        store.finish(p);
    }

    private final class Execution {
        final EndpointProbe p;
        final EndpointRevision r;
        final RemoteFileClient client;
        final Set<String> residuals = new LinkedHashSet<>();
        Execution(EndpointProbe p, EndpointRevision r, RemoteFileClient client) { this.p = p; this.r = r; this.client = client; }

        void progress() {
            p.residuals = String.join("\n", residuals);
            if (!store.progress(p.id, p.details, p.residuals)) throw new Stopped();
        }
        boolean stage(String name, IoAction action) {
            progress();
            try { action.run(); p.details += "\n" + name + ": OK"; return true; }
            catch (IOException | IllegalArgumentException e) { p.details += "\n" + name + ": " + reason(e); return false; }
            finally { progress(); }
        }

        void probe() {
            p.details = "CONNECT: OK\nPATH: " + client.pathAssurance();
            p.capacity = client.capacity();
            if (r.readRoot != null) {
                boolean listed = stage("LIST", () -> client.list(r.readRoot));
                if (listed && r.readTestFile != null) p.canRead = stage("READ", () -> {
                    String path = ControlledPaths.resolve(r.readRoot, r.readTestFile);
                    RemoteFileClient.Entry entry = client.lstat(path);
                    if (!entry.regular || entry.link || entry.size <= 0) throw new IOException("NONEMPTY_REGULAR_TEST_OBJECT_REQUIRED");
                    Digest sink = new Digest(Long.MAX_VALUE); client.read(path, sink);
                    if (sink.count != entry.size) throw new IOException("SIZE_MISMATCH");
                });
                else p.details += "\nREAD: NOT_TESTED_SPECIFY_TEST_OBJECT";
            }
            if (r.writeRoot == null || r.probeDirectory == null) {
                p.details += "\nWRITE: NOT_TESTED_NO_EXPLICIT_PROBE_DIRECTORY";
                progress(); return;
            }
            String base = ControlledPaths.resolve(r.writeRoot, r.probeDirectory);
            if (!stage("PROBE_DIRECTORY", () -> {
                if (!client.lstat(base).directory) throw new IOException("NOT_DIRECTORY");
            })) return;
            String directory = base + "/.sharing-probe-" + UUID.randomUUID().toString().replace("-", "");
            residuals.add(directory); progress();
            if (!stage("MKDIR", () -> client.mkdir(directory))) return; // uncertain creation is retained for operator review
            String payload = directory + "/payload", renamed = directory + "/renamed";
            byte[] bytes = new byte[4096]; new SecureRandom().nextBytes(bytes);
            try {
                residuals.add(payload); progress();
                p.canWrite = stage("WRITE", () -> client.write(payload, new ByteArrayInputStream(bytes)));
                if (p.canWrite) {
                    p.verified = stage("SIZE_AND_READBACK_SHA256", () -> {
                        if (client.lstat(payload).size != bytes.length) throw new IOException("SIZE_MISMATCH");
                        Digest sink = new Digest(bytes.length); client.read(payload, sink);
                        if (sink.count != bytes.length || !MessageDigest.isEqual(sink.hash.digest(), sha256().digest(bytes)))
                            throw new IOException("CONTENT_MISMATCH");
                    });
                    residuals.add(renamed); progress();
                    p.canRename = stage("RENAME", () -> {
                        client.rename(payload, renamed);
                        if (client.lstat(renamed).size != bytes.length) throw new IOException("RENAME_RESULT_UNVERIFIED");
                    });
                    if (p.canRename) residuals.remove(payload);
                }
            } finally {
                for (String path : new ArrayList<>(residuals)) {
                    if (path.equals(directory)) continue;
                    if (stage("CLEANUP_FILE", () -> client.deleteTemporary(path, false))) residuals.remove(path);
                }
                if (stage("CLEANUP_DIRECTORY", () -> client.deleteTemporary(directory, true))) {
                    // A successful non-recursive rmdir proves the directory (and all candidate files) no longer exists.
                    residuals.clear();
                }
                progress();
            }
        }
    }

    private static String reason(Exception e) {
        if (e instanceof UnknownHostException) return "DNS_FAILED";
        if (e instanceof SocketTimeoutException || e instanceof java.io.InterruptedIOException) return "TIMEOUT";
        if (e instanceof ConnectException) return "CONNECTION_FAILED";
        String message = e.getMessage();
        if (message != null && message.matches("[A-Z][A-Z0-9_]{1,100}")) return message;
        if (e instanceof org.apache.sshd.sftp.common.SftpException)
            return "SFTP_STATUS_" + ((org.apache.sshd.sftp.common.SftpException) e).getStatus();
        return e.getClass().getSimpleName();
    }
    private static MessageDigest sha256() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException("SHA256_UNAVAILABLE"); }
    }
    private static final class Digest extends OutputStream {
        final MessageDigest hash = sha256(); final long limit; long count;
        Digest(long limit) { this.limit = limit; }
        @Override public void write(int b) throws IOException { write(new byte[] {(byte) b}, 0, 1); }
        @Override public void write(byte[] bytes, int offset, int length) throws IOException {
            if (count > limit - length) throw new IOException("READBACK_TOO_LARGE");
            count += length; hash.update(bytes, offset, length);
        }
    }
    @FunctionalInterface private interface IoAction { void run() throws IOException; }
    private static final class Stopped extends RuntimeException { }
    @PreDestroy public void close() { workers.shutdownNow(); timer.shutdownNow(); }
}
