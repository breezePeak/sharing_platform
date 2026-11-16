package com.siwei.endpoint;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.*;
import com.siwei.protocol.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class FtpFinalReplyTest {
    @Test void aClosedDataStreamWithNegativeFinalReplyIsStillFailure() throws Exception {
        try (NegativeReplyServer server = new NegativeReplyServer()) {
            EndpointSettings settings = new EndpointSettings(); settings.setMasterKey(Base64.getEncoder().encodeToString(new byte[32]));
            CredentialCipher cipher = new CredentialCipher(settings);
            EndpointRevision r = new EndpointRevision(); r.id = "test"; r.protocol = EndpointRevision.Protocol.FTP;
            r.auth = EndpointRevision.Auth.PASSWORD; r.host = "127.0.0.1"; r.port = server.socket.getLocalPort();
            r.username = "tester"; r.secret = cipher.encrypt("test-only", r.id);
            r.readRoot = "/"; r.writeRoot = "/"; r.timeoutSeconds = 2; r.passive = true; r.isolatedFtpRoot = true;
            RemoteFileClientFactory factory = new RemoteFileClientFactory(cipher, settings);
            try (RemoteFileClient client = factory.connect(r)) {
                ByteArrayOutputStream target = new ByteArrayOutputStream();
                assertThatThrownBy(() -> client.read("/sample.bin", target)).isInstanceOf(IOException.class).hasMessage("READ_COMPLETE_FTP_451");
                assertThat(target.toByteArray()).isEqualTo(new byte[] {1, 2, 3});
            }
            try (RemoteFileClient client = factory.connect(r)) {
                assertThatThrownBy(() -> client.write("/sample.bin", new ByteArrayInputStream(new byte[] {1, 2, 3})))
                        .isInstanceOf(IOException.class).hasMessage("WRITE_COMPLETE_FTP_451");
            }
        }
    }

    /** Wire-level FTP fault fixture: transfer bytes, then explicitly reject completion. */
    private static final class NegativeReplyServer implements AutoCloseable {
        final ServerSocket socket = new ServerSocket(0, 10, InetAddress.getLoopbackAddress());
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final Future<?> worker;
        NegativeReplyServer() throws IOException { worker = executor.submit(() -> {
            try { for (int i = 0; i < 2; i++) serve(); }
            catch (IOException e) { throw new UncheckedIOException(e); }
        }); }
        void serve() throws IOException {
            try (Socket control = socket.accept();
                 BufferedReader input = new BufferedReader(new InputStreamReader(control.getInputStream(), StandardCharsets.UTF_8));
                 PrintWriter output = new PrintWriter(new OutputStreamWriter(control.getOutputStream(), StandardCharsets.UTF_8), true)) {
                control.setSoTimeout(5000); output.print("220 fixture\r\n"); output.flush();
                ServerSocket data = null;
                try {
                    String line;
                    while ((line = input.readLine()) != null) {
                        String command = line.split(" ", 2)[0];
                        switch (command) {
                            case "USER": reply(output, "331 password"); break;
                            case "PASS": reply(output, "230 logged in"); break;
                            case "TYPE": reply(output, "200 binary"); break;
                            case "MLST": reply(output, "250-Listing\r\n type=file;size=3; sample.bin\r\n250 End"); break;
                            case "PASV":
                                data = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
                                reply(output, "227 Entering Passive Mode (127,0,0,1," + data.getLocalPort() / 256 + "," + data.getLocalPort() % 256 + ")"); break;
                            case "RETR": case "STOR":
                                reply(output, "150 transfer");
                                if (data == null) throw new IOException("No passive socket");
                                try (Socket channel = data.accept()) {
                                    channel.setSoTimeout(5000);
                                    if (command.equals("RETR")) channel.getOutputStream().write(new byte[] {1, 2, 3});
                                    else channel.getInputStream().transferTo(OutputStream.nullOutputStream());
                                }
                                data.close(); data = null; reply(output, "451 final completion rejected"); break;
                            default: reply(output, "200 ok");
                        }
                    }
                } finally { if (data != null) data.close(); }
            }
        }
        void reply(PrintWriter output, String value) { output.print(value + "\r\n"); output.flush(); }
        @Override public void close() throws Exception {
            socket.close(); executor.shutdownNow(); worker.get(10, TimeUnit.SECONDS);
        }
    }
}
