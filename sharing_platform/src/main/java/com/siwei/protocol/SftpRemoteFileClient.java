package com.siwei.protocol;

import java.io.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import com.siwei.endpoint.EndpointRevision;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.config.hosts.HostConfigEntryResolver;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.config.keys.KeyUtils;
import org.apache.sshd.common.keyprovider.FileKeyPairProvider;
import org.apache.sshd.common.keyprovider.KeyIdentityProvider;
import org.apache.sshd.core.CoreModuleProperties;
import org.apache.sshd.sftp.client.*;

final class SftpRemoteFileClient extends ScopedRemoteClient {
    private final SshClient ssh = SshClient.setUpDefaultClient();
    private ClientSession session;
    private SftpClient sftp;

    SftpRemoteFileClient(EndpointRevision config, String password, Path key) throws IOException {
        super(config);
        AtomicBoolean rejectedHost = new AtomicBoolean();
        // Never inherit ~/.ssh identities, host overrides, proxies, or trust-on-first-use.
        ssh.setHostConfigEntryResolver(HostConfigEntryResolver.EMPTY);
        ssh.setKeyIdentityProvider(KeyIdentityProvider.EMPTY_KEYS_PROVIDER);
        ssh.setServerKeyVerifier((client, address, publicKey) -> {
            boolean trusted = config.fingerprint != null && config.fingerprint.equals(KeyUtils.getFingerPrint(publicKey));
            rejectedHost.set(!trusted);
            return trusted;
        });
        CoreModuleProperties.IDLE_TIMEOUT.set(ssh, Duration.ofSeconds(config.timeoutSeconds));
        CoreModuleProperties.AUTH_TIMEOUT.set(ssh, Duration.ofSeconds(config.timeoutSeconds));
        org.apache.sshd.sftp.SftpModuleProperties.SFTP_CHANNEL_OPEN_TIMEOUT.set(ssh, Duration.ofSeconds(config.timeoutSeconds));
        org.apache.sshd.sftp.client.impl.AbstractSftpClient.SFTP_CLIENT_CMD_TIMEOUT.set(ssh, Duration.ofSeconds(config.timeoutSeconds));
        try {
            ssh.start();
            session = ssh.connect(config.username, config.host, config.port).verify(config.timeoutSeconds * 1000L).getSession();
            if (config.auth == EndpointRevision.Auth.KEY) {
                FileKeyPairProvider provider = new FileKeyPairProvider(key);
                provider.setPasswordFinder((context, resource, retry) -> password);
                for (java.security.KeyPair pair : provider.loadKeys(session)) session.addPublicKeyIdentity(pair);
            } else session.addPasswordIdentity(password);
            session.auth().verify(config.timeoutSeconds * 1000L);
            sftp = SftpClientFactory.instance().createSftpClient(session);
        } catch (IOException | RuntimeException e) {
            close();
            if (rejectedHost.get()) throw new IOException("HOST_KEY_REJECTED");
            if (session != null && !session.isAuthenticated()) throw new IOException("AUTHENTICATION_OR_HANDSHAKE_FAILED");
            throw e;
        }
    }

    @Override protected void checkAncestors(String path) throws IOException {
        // shortcut: SFTP v3 cannot make lstat and open atomic; require managed directories until no-follow support is negotiated.
        String current = "";
        for (String part : path.substring(1).split("/")) {
            if (part.isEmpty()) continue;
            current += "/" + part;
            SftpClient.Attributes a = sftp.lstat(current);
            if (a.isSymbolicLink() || (!a.isDirectory() && !a.isRegularFile())) throw new IOException("LINK_OR_SPECIAL_FILE");
        }
    }
    private Entry entry(String name, SftpClient.Attributes a) {
        return new Entry(name, a.getSize(), a.isDirectory(), a.isRegularFile(), a.isSymbolicLink());
    }
    @Override public Entry lstat(String path) throws IOException { authorize(path, false, true); return entry(path, sftp.lstat(path)); }
    @Override public List<Entry> list(String path) throws IOException {
        authorize(path, false, true);
        List<Entry> entries = new ArrayList<>();
        try (SftpClient.CloseableHandle handle = sftp.openDir(path)) {
            List<SftpClient.DirEntry> batch;
            while ((batch = sftp.readDir(handle)) != null) {
                for (SftpClient.DirEntry e : batch) entries.add(entry(e.getFilename(), e.getAttributes()));
            }
        }
        return entries;
    }
    @Override public void read(String path, OutputStream target) throws IOException {
        authorize(path, false, true);
        if (!sftp.lstat(path).isRegularFile()) throw new IOException("NOT_REGULAR_FILE");
        try (InputStream source = sftp.read(path)) { source.transferTo(target); }
    }
    @Override public void write(String path, InputStream source) throws IOException {
        authorize(path, true, false);
        try (OutputStream target = sftp.write(path, SftpClient.OpenMode.Write, SftpClient.OpenMode.Create, SftpClient.OpenMode.Exclusive)) {
            source.transferTo(target);
        }
    }
    @Override public void mkdir(String path) throws IOException { authorize(path, true, false); sftp.mkdir(path); }
    @Override public void rename(String source, String target) throws IOException {
        authorize(source, true, true); authorize(target, true, false); sftp.rename(source, target);
    }
    @Override public void deleteTemporary(String path, boolean directory) throws IOException {
        temporary(path); authorize(path, true, true);
        if (directory) sftp.rmdir(path); else sftp.remove(path);
    }
    @Override public String pathAssurance() { return "SFTP_LSTAT_NO_LINKS"; }
    @Override public String capacity() {
        try {
            String path = config.writeRoot != null ? config.writeRoot : config.readRoot;
            authorize(path, false, true);
            var stat = sftp.getExtension(org.apache.sshd.sftp.client.extensions.openssh.OpenSSHStatPathExtension.class);
            if (stat != null && stat.isSupported()) {
                var info = stat.stat(path);
                if (info.f_bavail >= 0 && info.f_frsize > 0) return "BYTES:" + Math.multiplyExact(info.f_bavail, info.f_frsize);
            }
            var space = sftp.getExtension(org.apache.sshd.sftp.client.extensions.SpaceAvailableExtension.class);
            if (space != null && space.isSupported()) {
                long available = space.available(path).unusedBytesAvailableToUser;
                if (available >= 0) return "BYTES:" + available;
            }
        } catch (IOException | RuntimeException ignored) { /* an unavailable capacity extension is not evidence of free space */ }
        return "UNKNOWN";
    }
    @Override public void close() {
        if (session != null) session.close(true);
        ssh.close(true);
    }
}
