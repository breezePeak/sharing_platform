package com.siwei.endpoint;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import com.siwei.ProtocolServers;
import com.siwei.protocol.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import static org.assertj.core.api.Assertions.*;

class ProtocolTest {
    @TempDir Path root;

    private EndpointSettings settings() {
        EndpointSettings s = new EndpointSettings(); s.setMasterKey(Base64.getEncoder().encodeToString(new byte[32]));
        s.setKeyDirectory(root.toString()); return s;
    }
    private EndpointRevision revision(ProtocolServers server, EndpointRevision.Protocol protocol, CredentialCipher cipher) {
        EndpointRevision r = new EndpointRevision();
        r.id = UUID.randomUUID().toString(); r.protocol = protocol; r.host = "127.0.0.1"; r.port = server.port;
        r.username = "tester"; r.auth = EndpointRevision.Auth.PASSWORD;
        r.secret = cipher.encrypt(ProtocolServers.PASSWORD, r.id); r.fingerprint = server.fingerprint;
        r.readRoot = "/"; r.writeRoot = "/"; r.probeDirectory = "probe";
        r.timeoutSeconds = 2; r.passive = true; r.isolatedFtpRoot = true; return r;
    }
    @ParameterizedTest @EnumSource(EndpointRevision.Protocol.class)
    void realProtocolsTransferBinaryRenameAndCleanWithoutDeletingBusinessData(EndpointRevision.Protocol protocol) throws Exception {
        EndpointSettings s = settings(); CredentialCipher cipher = new CredentialCipher(s);
        try (ProtocolServers server = new ProtocolServers(root, protocol, ProtocolServers.Mode.NORMAL)) {
            EndpointRevision r = revision(server, protocol, cipher);
            byte[] bytes = new byte[65537]; new Random(42).nextBytes(bytes);
            String directory = "/probe/.sharing-probe-" + UUID.randomUUID().toString().replace("-", "");
            try (RemoteFileClient client = new RemoteFileClientFactory(cipher, s).connect(r)) {
                assertThat(client.list("/")).anyMatch(e -> e.name.equals("sample.bin"));
                client.mkdir(directory); client.write(directory + "/payload", new ByteArrayInputStream(bytes));
                assertThat(client.lstat(directory + "/payload").size).isEqualTo(bytes.length);
                ByteArrayOutputStream readback = new ByteArrayOutputStream(); client.read(directory + "/payload", readback);
                assertThat(readback.toByteArray()).isEqualTo(bytes);
                client.rename(directory + "/payload", directory + "/renamed");
                client.deleteTemporary(directory + "/renamed", false); client.deleteTemporary(directory, true);
                assertThatThrownBy(() -> client.deleteTemporary("/sample.bin", false)).isInstanceOf(IOException.class);
                assertThatThrownBy(() -> client.read("/../outside", new ByteArrayOutputStream())).isInstanceOf(IllegalArgumentException.class);
            }
            try (var files = Files.list(root.resolve("probe"))) { assertThat(files.count()).isZero(); }
            assertThat(Files.readString(root.resolve("sample.bin"))).contains("non-empty");
        }
    }
    @Test void sftpRejectsUnknownOrWrongHostKeyAndWrongCredentials() throws Exception {
        EndpointSettings s = settings(); CredentialCipher cipher = new CredentialCipher(s);
        try (ProtocolServers server = new ProtocolServers(root, EndpointRevision.Protocol.SFTP, ProtocolServers.Mode.NORMAL)) {
            EndpointRevision r = revision(server, EndpointRevision.Protocol.SFTP, cipher);
            RemoteFileClientFactory factory = new RemoteFileClientFactory(cipher, s);
            r.fingerprint = null;
            assertThatThrownBy(() -> factory.connect(r)).isInstanceOf(IOException.class).hasMessageContaining("HOST_KEY_REJECTED");
            r.fingerprint = "SHA256:" + "A".repeat(43);
            assertThatThrownBy(() -> factory.connect(r)).isInstanceOf(IOException.class).hasMessageContaining("HOST_KEY_REJECTED");
            r.fingerprint = server.fingerprint; r.secret = cipher.encrypt("wrong", r.id);
            assertThatThrownBy(() -> factory.connect(r)).isInstanceOf(IOException.class).hasMessageContaining("AUTHENTICATION");
        }
    }
    @Test void sftpUsesOnlyReferencedPrivateKeyAndRejectsPathEscape() throws Exception {
        EndpointSettings s = settings(); CredentialCipher cipher = new CredentialCipher(s);
        try (ProtocolServers server = new ProtocolServers(root, EndpointRevision.Protocol.SFTP, ProtocolServers.Mode.NORMAL)) {
            Path key = root.resolve("identity.pem");
            Files.writeString(key, "-----BEGIN PRIVATE KEY-----\n" + Base64.getMimeEncoder(64, new byte[] {'\n'})
                    .encodeToString(server.userKey.getPrivate().getEncoded()) + "\n-----END PRIVATE KEY-----\n");
            EndpointRevision r = revision(server, EndpointRevision.Protocol.SFTP, cipher);
            r.auth = EndpointRevision.Auth.KEY; r.secret = null; r.keyFile = "identity.pem";
            r.keyDigest = ControlledPaths.keyDigest(key);
            RemoteFileClientFactory factory = new RemoteFileClientFactory(cipher, s);
            try (RemoteFileClient client = factory.connect(r)) { assertThat(client.lstat("/sample.bin").regular).isTrue(); }
            Files.writeString(key, "replaced-key");
            assertThatThrownBy(() -> factory.connect(r)).isInstanceOf(IOException.class).hasMessage("PRIVATE_KEY_CHANGED_RECONFIGURE_REQUIRED");
            r.keyFile = "../identity.pem";
            assertThatThrownBy(() -> factory.connect(r)).isInstanceOf(IllegalArgumentException.class);
        }
    }
    @ParameterizedTest @EnumSource(EndpointRevision.Protocol.class)
    void readOnlyConnectionsCanActuallyReadAndBrokenDataDoesNotPass(EndpointRevision.Protocol protocol) throws Exception {
        EndpointSettings s = settings(); CredentialCipher cipher = new CredentialCipher(s);
        try (ProtocolServers server = new ProtocolServers(root, protocol, ProtocolServers.Mode.READ_ONLY)) {
            try (RemoteFileClient client = new RemoteFileClientFactory(cipher, s).connect(revision(server, protocol, cipher))) {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream(); client.read("/sample.bin", bytes);
                assertThat(bytes.size()).isGreaterThan(0);
                assertThatThrownBy(() -> client.mkdir("/probe/forbidden")).isInstanceOf(IOException.class);
            }
        }
    }
    @Test void remoteLinkAttributesAreRejectedOverRealSftp() throws Exception {
        EndpointSettings s = settings(); CredentialCipher cipher = new CredentialCipher(s);
        try (ProtocolServers server = new ProtocolServers(root, EndpointRevision.Protocol.SFTP, ProtocolServers.Mode.LINK_ATTRIBUTES)) {
            try (RemoteFileClient client = new RemoteFileClientFactory(cipher, s).connect(revision(server, EndpointRevision.Protocol.SFTP, cipher))) {
                assertThatThrownBy(() -> client.read("/link.bin", new ByteArrayOutputStream())).isInstanceOf(IOException.class).hasMessage("LINK_OR_SPECIAL_FILE");
            }
        }
    }
}
