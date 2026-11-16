package com.siwei.protocol;

import java.io.IOException;
import java.nio.file.Path;
import com.siwei.endpoint.*;
import org.springframework.stereotype.Component;

@Component
public class RemoteFileClientFactory {
    private final CredentialCipher cipher;
    private final EndpointSettings settings;
    public RemoteFileClientFactory(CredentialCipher cipher, EndpointSettings settings) { this.cipher = cipher; this.settings = settings; }

    public RemoteFileClient connect(EndpointRevision config) throws IOException {
        if (config.auth == EndpointRevision.Auth.PASSWORD && !config.hasSecret()) throw new IOException("CREDENTIAL_MISSING");
        if (config.auth == EndpointRevision.Auth.KEY && config.keyFile == null) throw new IOException("CREDENTIAL_MISSING");
        String password = cipher.decrypt(config);
        if (config.protocol == EndpointRevision.Protocol.FTP) return new FtpRemoteFileClient(config, password);
        Path key = config.auth == EndpointRevision.Auth.KEY ? ControlledPaths.keyFile(settings.getKeyDirectory(), config.keyFile) : null;
        if (key != null && (config.keyDigest == null || !config.keyDigest.equals(ControlledPaths.keyDigest(key))))
            throw new IOException("PRIVATE_KEY_CHANGED_RECONFIGURE_REQUIRED");
        return new SftpRemoteFileClient(config, password, key);
    }
}
