package com.siwei.protocol;

import java.io.*;
import java.time.Duration;
import java.util.*;
import com.siwei.endpoint.EndpointRevision;
import org.apache.commons.net.ftp.*;

final class FtpRemoteFileClient extends ScopedRemoteClient {
    private final FTPClient ftp = new FTPClient();
    FtpRemoteFileClient(EndpointRevision config, String password) throws IOException {
        super(config);
        if (!config.isolatedFtpRoot) throw new IOException("FTP_ISOLATED_ACCOUNT_REQUIRED");
        ftp.setConnectTimeout(config.timeoutSeconds * 1000);
        ftp.setDefaultTimeout(config.timeoutSeconds * 1000);
        ftp.setDataTimeout(Duration.ofSeconds(config.timeoutSeconds));
        ftp.setControlEncoding("UTF-8");
        try {
            ftp.connect(config.host, config.port);
            ftp.setSoTimeout(config.timeoutSeconds * 1000);
            require(FTPReply.isPositiveCompletion(ftp.getReplyCode()), "CONNECT");
            require(ftp.login(config.username, password), "AUTHENTICATION");
            require(ftp.setFileType(FTP.BINARY_FILE_TYPE), "BINARY_TYPE");
            if (config.passive) ftp.enterLocalPassiveMode(); else ftp.enterLocalActiveMode();
        } catch (IOException | RuntimeException e) { close(); throw e; }
    }

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
    @Override public void close() throws IOException { if (ftp.isConnected()) ftp.disconnect(); }
}
