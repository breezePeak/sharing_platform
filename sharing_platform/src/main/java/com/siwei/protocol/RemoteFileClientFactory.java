package com.siwei.protocol;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.concurrent.*;
import javax.annotation.PreDestroy;
import com.siwei.endpoint.*;
import org.springframework.stereotype.Component;

@Component
public class RemoteFileClientFactory {
    private final CredentialCipher cipher;
    private final EndpointSettings settings;
    private final java.util.Set<RemoteFileClient> active = ConcurrentHashMap.newKeySet();
    private final ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1, runnable -> {
        Thread thread = new Thread(runnable, "remote-inactivity"); thread.setDaemon(true); return thread;
    });
    public RemoteFileClientFactory(CredentialCipher cipher, EndpointSettings settings) {
        this.cipher = cipher; this.settings = settings; timer.setRemoveOnCancelPolicy(true);
    }

    public RemoteFileClient connect(EndpointRevision config) throws IOException {
        return connect(config, client -> {}, true);
    }

    public RemoteFileClient connect(EndpointRevision config, Consumer<RemoteFileClient> onCreate) throws IOException {
        return connect(config, onCreate, false); // Probes own their existing total deadline and cancellation callback.
    }

    private RemoteFileClient connect(EndpointRevision config, Consumer<RemoteFileClient> onCreate, boolean timed) throws IOException {
        if (config.auth == EndpointRevision.Auth.PASSWORD && !config.hasSecret()) throw new IOException("CREDENTIAL_MISSING");
        if (config.auth == EndpointRevision.Auth.KEY && config.keyFile == null) throw new IOException("CREDENTIAL_MISSING");
        String password = cipher.decrypt(config);
        if (config.protocol == EndpointRevision.Protocol.FTP) {
            FtpRemoteFileClient client = new FtpRemoteFileClient(config, password);
            return connect(client, config.timeoutSeconds, timed, onCreate, () -> client.connect(config, password));
        }
        Path key = config.auth == EndpointRevision.Auth.KEY ? ControlledPaths.keyFile(settings.getKeyDirectory(), config.keyFile) : null;
        if (key != null && (config.keyDigest == null || !config.keyDigest.equals(ControlledPaths.keyDigest(key))))
            throw new IOException("PRIVATE_KEY_CHANGED_RECONFIGURE_REQUIRED");
        SftpRemoteFileClient client = new SftpRemoteFileClient(config, password, key);
        return connect(client, config.timeoutSeconds, timed, onCreate, () -> client.connect(config, password, key));
    }

    private RemoteFileClient connect(RemoteFileClient underlying, int timeout, boolean timed, Consumer<RemoteFileClient> onCreate, Connection start) throws IOException {
        TimedRemoteFileClient deadline = timed ? new TimedRemoteFileClient(underlying, timer, timeout, active::remove) : null;
        RemoteFileClient client = timed ? deadline : underlying;
        if (timed) active.add(client);
        try {
            onCreate.accept(client);
            if (timed) deadline.io(() -> { start.connect(); return null; }); else start.connect();
            return client;
        }
        catch (IOException | RuntimeException e) {
            try { client.close(); } catch (IOException closing) { e.addSuppressed(closing); }
            throw e;
        }
    }
    @FunctionalInterface private interface Connection { void connect() throws IOException; }
    @PreDestroy public void close() {
        for (RemoteFileClient client : active) try { client.close(); } catch (IOException ignored) { }
        timer.shutdownNow();
    }
}
