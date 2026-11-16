package com.siwei;

import java.io.*;
import java.nio.file.*;
import java.security.*;
import java.util.*;
import com.siwei.endpoint.*;
import org.apache.ftpserver.*;
import org.apache.ftpserver.ftplet.*;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.impl.*;
import org.apache.sshd.common.config.keys.KeyUtils;
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import org.apache.sshd.common.keyprovider.KeyPairProvider;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.session.ServerSession;
import org.apache.sshd.sftp.server.*;

/** Real servers, loopback only, disposable credentials and files. No Docker/customer endpoints. */
public final class ProtocolServers implements AutoCloseable {
    public enum Mode { NORMAL, READ_ONLY, WRITE_ONLY, BROKEN_DATA, CLEANUP_FAIL, RENAME_FAIL, STALL_READ, SLOW_COMMANDS, LINK_ATTRIBUTES }
    public static final String PASSWORD = "test-only-password";
    public final Path root;
    public final int port;
    public final String fingerprint;
    public final KeyPair hostKey;
    public final KeyPair userKey;
    private FtpServer ftp;
    private SshServer ssh;

    public ProtocolServers(Path root, EndpointRevision.Protocol protocol, Mode mode) throws Exception {
        this.root = root;
        Files.createDirectories(root.resolve("probe"));
        Files.writeString(root.resolve("sample.bin"), "non-empty test content\r\n\u0000\u00ff");
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA"); generator.initialize(2048);
        hostKey = generator.generateKeyPair(); userKey = generator.generateKeyPair();
        fingerprint = KeyUtils.getFingerPrint(hostKey.getPublic());
        if (protocol == EndpointRevision.Protocol.FTP) {
            FtpServerFactory factory = new FtpServerFactory();
            ListenerFactory listener = new ListenerFactory(); listener.setServerAddress("127.0.0.1"); listener.setPort(0);
            factory.addListener("default", listener.createListener());
            BaseUser user = new BaseUser(); user.setName("tester"); user.setPassword(PASSWORD);
            user.setHomeDirectory(root.toString()); user.setAuthorities(Collections.singletonList(new WritePermission()));
            factory.getUserManager().save(user);
            factory.setFtplets(new HashMap<>(Collections.singletonMap("faults", new DefaultFtplet() {
                @Override public FtpletResult beforeCommand(FtpSession session, FtpRequest request) throws IOException, FtpException {
                    String command = request.getCommand();
                    if (mode == Mode.SLOW_COMMANDS && Arrays.asList("MLST", "LIST", "MKD", "STOR", "RETR", "RNFR", "RNTO", "DELE", "RMD").contains(command)) {
                        try { Thread.sleep(750); } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IOException("test interrupted"); }
                    }
                    boolean deny = mode == Mode.READ_ONLY && Arrays.asList("STOR", "MKD", "RNFR", "RNTO", "DELE", "RMD").contains(command)
                            || mode == Mode.WRITE_ONLY && command.equals("RETR")
                            || mode == Mode.BROKEN_DATA && Arrays.asList("LIST", "MLSD", "RETR", "STOR").contains(command)
                            || mode == Mode.CLEANUP_FAIL && Arrays.asList("DELE", "RMD").contains(command)
                            || mode == Mode.RENAME_FAIL && command.equals("RNTO");
                    if (mode == Mode.STALL_READ && command.equals("RETR")) {
                        session.write(new DefaultFtpReply(150, "test stalled data connection"));
                        return FtpletResult.SKIP;
                    }
                    if (deny) { session.write(new DefaultFtpReply(mode == Mode.BROKEN_DATA ? 425 : 550, "test restriction")); return FtpletResult.SKIP; }
                    return FtpletResult.DEFAULT;
                }
            })));
            ftp = factory.createServer(); ftp.start();
            port = factory.getListener("default").getPort();
        } else {
            ssh = SshServer.setUpDefaultServer(); ssh.setHost("127.0.0.1"); ssh.setPort(0);
            ssh.setKeyPairProvider(KeyPairProvider.wrap(hostKey));
            ssh.setPasswordAuthenticator((user, password, session) -> user.equals("tester") && password.equals(PASSWORD));
            ssh.setPublickeyAuthenticator((user, key, session) -> user.equals("tester") && KeyUtils.compareKeys(key, userKey.getPublic()));
            ssh.setFileSystemFactory(new VirtualFileSystemFactory(root));
            SftpSubsystemFactory subsystem = new SftpSubsystemFactory.Builder().build();
            if (mode == Mode.LINK_ATTRIBUTES) {
                Files.writeString(root.resolve("link.bin"), "link attribute fault fixture");
                subsystem.setFileSystemAccessor(new SftpFileSystemAccessor() {
                    @Override public Map<String, ?> readFileAttributes(SftpSubsystemProxy proxy, Path file,
                            String view, LinkOption... options) throws IOException {
                        Map<String, Object> attributes = new HashMap<>(SftpFileSystemAccessor.super.readFileAttributes(proxy, file, view, options));
                        if (file.getFileName().toString().equals("link.bin")) {
                            attributes.put("isSymbolicLink", true); attributes.put("isRegularFile", false); attributes.put("isDirectory", false);
                        }
                        return attributes;
                    }
                });
            }
            subsystem.addSftpEventListener(new SftpEventListener() {
                @Override public void opening(ServerSession session, String remote, Handle handle) throws IOException {
                    if (handle instanceof FileHandle) {
                        Set<StandardOpenOption> options = ((FileHandle) handle).getOpenOptions();
                        if (mode == Mode.READ_ONLY && options.contains(StandardOpenOption.WRITE)
                                || mode == Mode.WRITE_ONLY && options.contains(StandardOpenOption.READ)) throw new AccessDeniedException("test");
                    }
                }
                @Override public void creating(ServerSession session, Path path, Map<String, ?> attrs) throws IOException {
                    if (mode == Mode.READ_ONLY) throw new AccessDeniedException("test");
                }
                @Override public void removing(ServerSession session, Path path, boolean directory) throws IOException {
                    if (mode == Mode.CLEANUP_FAIL || mode == Mode.READ_ONLY) throw new AccessDeniedException("test");
                }
                @Override public void moving(ServerSession session, Path source, Path target, Collection<CopyOption> options) throws IOException {
                    if (mode == Mode.RENAME_FAIL || mode == Mode.READ_ONLY) throw new AccessDeniedException("test");
                }
            });
            ssh.setSubsystemFactories(Collections.singletonList(subsystem)); ssh.start(); port = ssh.getPort();
        }
    }

    public EndpointForm form(EndpointRevision.Protocol protocol) {
        EndpointForm f = new EndpointForm();
        f.name = "local test"; f.protocol = protocol; f.host = "127.0.0.1"; f.port = port;
        f.username = "tester"; f.password = PASSWORD; f.readRoot = "/"; f.readTestFile = "sample.bin";
        f.writeRoot = "/"; f.probeDirectory = "probe"; f.timeoutSeconds = 2; f.isolatedFtpRoot = true;
        if (protocol == EndpointRevision.Protocol.SFTP) f.fingerprint = fingerprint;
        return f;
    }

    @Override public void close() throws IOException {
        if (ftp != null) ftp.stop();
        if (ssh != null) ssh.stop(true);
    }
}
