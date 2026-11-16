package com.siwei.protocol;

import java.io.*;
import java.net.*;
import java.time.Duration;
import java.util.*;
import javax.net.ServerSocketFactory;
import com.siwei.endpoint.EndpointRevision;
import org.apache.commons.net.DefaultSocketFactory;
import org.apache.commons.net.ftp.*;

final class FtpRemoteFileClient extends ScopedRemoteClient {
    private final FTPClient ftp = new FTPClient();
    private final Set<Closeable> sockets = new HashSet<>();
    private boolean closed;
    FtpRemoteFileClient(EndpointRevision config, String password) throws IOException {
        super(config);
        if (!config.isolatedFtpRoot) throw new IOException("FTP_ISOLATED_ACCOUNT_REQUIRED");
        // Commons Net 3.13 uses this factory for both control and passive data sockets, before connect/handshake.
        ftp.setSocketFactory(new DefaultSocketFactory() {
            @Override public Socket createSocket() throws IOException { return newSocket(); }
        });
        ftp.setServerSocketFactory(new ServerSocketFactory() {
            @Override public ServerSocket createServerSocket(int port) throws IOException { return createServerSocket(port, 50, null); }
            @Override public ServerSocket createServerSocket(int port, int backlog) throws IOException { return createServerSocket(port, backlog, null); }
            @Override public ServerSocket createServerSocket(int port, int backlog, InetAddress address) throws IOException {
                ServerSocket server = track(new ServerSocket() {
                    @Override public Socket accept() throws IOException {
                        return track(super.accept());
                    }
                    @Override public void close() throws IOException {
                        try { super.close(); } finally { untrack(this); }
                    }
                });
                try { server.bind(new InetSocketAddress(address, port), backlog); return server; }
                catch (IOException | RuntimeException failure) {
                    try { server.close(); } catch (IOException closing) { failure.addSuppressed(closing); }
                    throw failure;
                }
            }
        });
        ftp.setConnectTimeout(config.timeoutSeconds * 1000);
        ftp.setDefaultTimeout(config.timeoutSeconds * 1000);
        ftp.setDataTimeout(Duration.ofSeconds(config.timeoutSeconds));
        ftp.setControlEncoding("UTF-8");
    }

    void connect(EndpointRevision config, String password) throws IOException {
        try {
            ftp.connect(config.host, config.port);
            ftp.setSoTimeout(config.timeoutSeconds * 1000);
            require(FTPReply.isPositiveCompletion(ftp.getReplyCode()), "CONNECT");
            require(ftp.login(config.username, password), "AUTHENTICATION");
            require(ftp.setFileType(FTP.BINARY_FILE_TYPE), "BINARY_TYPE");
            if (config.passive) ftp.enterLocalPassiveMode(); else ftp.enterLocalActiveMode();
        } catch (IOException | RuntimeException e) {
            try { close(); } catch (IOException closing) { e.addSuppressed(closing); }
            throw e;
        }
    }

    private Socket newSocket() throws IOException {
        return track(new Socket() {
            @Override public void close() throws IOException {
                try { super.close(); } finally { untrack(this); }
            }
        });
    }
    private <T extends Closeable> T track(T socket) throws IOException {
        synchronized (sockets) {
            if (!closed) {
                sockets.removeIf(value -> value instanceof Socket && ((Socket) value).isClosed());
                sockets.add(socket); return socket;
            }
        }
        socket.close();
        throw new IOException("FTP_CLIENT_CLOSED");
    }
    private void untrack(Closeable socket) { synchronized (sockets) { sockets.remove(socket); } }

    private void require(boolean success, String stage) throws IOException {
        if (!success) throw new IOException(stage + "_FTP_" + ftp.getReplyCode());
    }
    private FTPFile raw(String path) throws IOException {
        FTPFile value = ftp.mlistFile(path);
        if (value == null) {
            FTPFile[] files = ftp.listFiles(path);
            require(FTPReply.isPositiveCompletion(ftp.getReplyCode()), "STAT");
            if (files.length != 1 || !files[0].getName().equals(path.substring(path.lastIndexOf('/') + 1)))
                throw new IOException("FTP_PATH_TYPE_UNVERIFIED");
            value = files[0];
        }
        return value;
    }
    @Override protected void checkAncestors(String path) throws IOException {
        if (path.equals("/")) return;
        String current = "";
        for (String part : path.substring(1).split("/")) {
            current += "/" + part;
            FTPFile file = raw(current);
            if (file.isSymbolicLink() || (!file.isDirectory() && !file.isFile())) throw new IOException("LINK_OR_SPECIAL_FILE");
        }
    }
    private Entry entry(FTPFile f) { return new Entry(f.getName(), f.getSize(), f.isDirectory(), f.isFile(), f.isSymbolicLink()); }
    @Override public Entry lstat(String path) throws IOException { authorize(path, false, true); return entry(raw(path)); }
    @Override public List<Entry> list(String path) throws IOException {
        authorize(path, false, true);
        FTPFile[] values = ftp.listFiles(path);
        require(FTPReply.isPositiveCompletion(ftp.getReplyCode()), "LIST");
        List<Entry> entries = new ArrayList<>();
        for (FTPFile f : values) entries.add(entry(f));
        return entries;
    }
    @Override public void read(String path, OutputStream target) throws IOException {
        authorize(path, false, true);
        if (!raw(path).isFile()) throw new IOException("NOT_REGULAR_FILE");
        InputStream stream = ftp.retrieveFileStream(path);
        require(stream != null, "READ_OPEN");
        try {
            try (InputStream input = stream) { input.transferTo(target); }
            require(ftp.completePendingCommand(), "READ_COMPLETE");
        } catch (IOException | RuntimeException failure) {
            try { close(); } catch (IOException closing) { failure.addSuppressed(closing); }
            throw failure;
        }
    }
    @Override public void write(String path, InputStream source) throws IOException {
        authorize(path, true, false);
        OutputStream stream = ftp.storeFileStream(path);
        require(stream != null, "WRITE_OPEN");
        try {
            try (OutputStream output = stream) { source.transferTo(output); }
            require(ftp.completePendingCommand(), "WRITE_COMPLETE");
        } catch (IOException | RuntimeException failure) {
            try { close(); } catch (IOException closing) { failure.addSuppressed(closing); }
            throw failure;
        }
    }
    @Override public void mkdir(String path) throws IOException { authorize(path, true, false); require(ftp.makeDirectory(path), "MKDIR"); }
    @Override public void rename(String source, String target) throws IOException {
        authorize(source, true, true); authorize(target, true, false);
        require(ftp.rename(source, target), "RENAME");
    }
    @Override public void deleteTemporary(String path, boolean directory) throws IOException {
        temporary(path); authorize(path, true, true);
        require(directory ? ftp.removeDirectory(path) : ftp.deleteFile(path), "CLEANUP");
    }
    @Override public String pathAssurance() { return "LIMITED_FTP_ISOLATED_ACCOUNT"; }
    @Override public void close() throws IOException {
        List<Closeable> active;
        synchronized (sockets) {
            if (closed) return;
            closed = true;
            active = new ArrayList<>(sockets);
            sockets.clear();
        }
        // Abort sockets first: closing buffered remote output here could flush and block the shared deadline thread.
        IOException failure = null;
        for (Closeable socket : active) {
            try { socket.close(); }
            catch (IOException e) { if (failure == null) failure = e; else failure.addSuppressed(e); }
        }
        try { ftp.disconnect(); }
        catch (IOException e) { if (failure == null) failure = e; else failure.addSuppressed(e); }
        if (failure != null) throw failure;
    }
}
