package com.siwei.protocol;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Consumer;
import com.siwei.endpoint.*;
import org.springframework.stereotype.Component;

@Component
public class RemoteFileClientFactory {
    private final CredentialCipher cipher;
    private final EndpointSettings settings;
    public RemoteFileClientFactory(CredentialCipher cipher, EndpointSettings settings) { this.cipher = cipher; this.settings = settings; }

    public RemoteFileClient connect(EndpointRevision config) throws IOException {
        return connect(config, client -> {});
    }

    public RemoteFileClient connect(EndpointRevision config, Consumer<RemoteFileClient> onCreate) throws IOException {
        if (config.auth == EndpointRevision.Auth.PASSWORD && !config.hasSecret()) throw new IOException("CREDENTIAL_MISSING");
        if (config.auth == EndpointRevision.Auth.KEY && config.keyFile == null) throw new IOException("CREDENTIAL_MISSING");
        String password = cipher.decrypt(config);
        if (config.protocol == EndpointRevision.Protocol.FTP) {
            FtpRemoteFileClient client = new FtpRemoteFileClient(config, password);
            return connect(client, onCreate, () -> client.connect(config, password));
        }
        Path key = config.auth == EndpointRevision.Auth.KEY ? ControlledPaths.keyFile(settings.getKeyDirectory(), config.keyFile) : null;
        if (key != null && (config.keyDigest == null || !config.keyDigest.equals(ControlledPaths.keyDigest(key))))
            throw new IOException("PRIVATE_KEY_CHANGED_RECONFIGURE_REQUIRED");
        SftpRemoteFileClient client = new SftpRemoteFileClient(config, password, key);
        return connect(client, onCreate, () -> client.connect(config, password, key));
    }

    private RemoteFileClient connect(RemoteFileClient client, Consumer<RemoteFileClient> onCreate, Connection start) throws IOException {
        try { onCreate.accept(client); start.connect(); return client; }
        catch (IOException | RuntimeException e) {
            try { client.close(); } catch (IOException closing) { e.addSuppressed(closing); }
            throw e;
        }
    }
    @FunctionalInterface private interface Connection { void connect() throws IOException; }
}
