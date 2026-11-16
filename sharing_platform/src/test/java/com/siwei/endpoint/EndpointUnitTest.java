package com.siwei.endpoint;

import java.nio.file.*;
import java.util.Base64;
import com.siwei.protocol.ControlledPaths;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;

class EndpointUnitTest {
    @TempDir Path directory;
    @Test void encryptionIsRandomizedBoundToRevisionAndFailsClosed() {
        EndpointSettings settings = new EndpointSettings(); settings.setMasterKey(Base64.getEncoder().encodeToString(new byte[32]));
        CredentialCipher cipher = new CredentialCipher(settings);
        EndpointRevision r = new EndpointRevision(); r.id = "version-one"; r.secret = cipher.encrypt("canary", r.id);
        assertThat(r.secret).doesNotContain("canary").isNotEqualTo(cipher.encrypt("canary", r.id));
        assertThat(cipher.decrypt(r)).isEqualTo("canary");
        r.id = "version-two";
        assertThatThrownBy(() -> cipher.decrypt(r)).isInstanceOf(IllegalStateException.class);
        settings.setMasterKey("");
        assertThatThrownBy(() -> cipher.encrypt("canary", "id")).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void pathsRejectTraversalControlCharactersAndArbitraryKeyFiles() throws Exception {
        for (String value : new String[] {"../escape", "x/../escape", "/absolute", "a//b", "a\\b", "a\r\nDELE /", "-option", "."})
            assertThatThrownBy(() -> ControlledPaths.relative(value)).isInstanceOf(IllegalArgumentException.class);
        assertThat(ControlledPaths.resolve("/allowed", "a.bin")).isEqualTo("/allowed/a.bin");
        assertThat(ControlledPaths.within("/allowed", "/allowed-other/a")).isFalse();
        Files.writeString(directory.resolve("key.pem"), "test-only");
        assertThat(Files.isSameFile(ControlledPaths.keyFile(directory.toString(), "key.pem"), directory.resolve("key.pem"))).isTrue();
        assertThatThrownBy(() -> ControlledPaths.keyFile(directory.toString(), "../key.pem")).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void localDirectoryLinksAreRejected() throws Exception {
        Path target = Files.createDirectory(directory.resolve("target"));
        Path link = directory.resolve("link");
        if (System.getProperty("os.name").startsWith("Windows")) {
            Process process = new ProcessBuilder("cmd.exe", "/d", "/c", "mklink", "/J", link.toString(), target.toString())
                    .redirectErrorStream(true).start();
            assertThat(process.waitFor()).isZero();
        } else Files.createSymbolicLink(link, target);
        try { assertThatThrownBy(() -> ControlledPaths.localDirectory(link.toString())).isInstanceOf(IllegalArgumentException.class); }
        finally { Files.delete(link); }
    }
    @Test void clearedPrivateKeyReferenceCannotConnect() {
        EndpointSettings settings = new EndpointSettings();
        EndpointRevision r = new EndpointRevision(); r.auth = EndpointRevision.Auth.KEY;
        assertThatThrownBy(() -> new com.siwei.protocol.RemoteFileClientFactory(new CredentialCipher(settings), settings).connect(r))
                .isInstanceOf(java.io.IOException.class).hasMessage("CREDENTIAL_MISSING");
    }
}
