package com.siwei.protocol;

import java.io.*;
import java.util.List;

/** One execution owns one session. Caller streams remain caller-owned. Paths use '/' on both protocols. */
public interface RemoteFileClient extends AutoCloseable {
    final class Entry {
        public final String name;
        public final long size;
        public final Long modifiedMillis;
        public final boolean directory, regular, link;
        public Entry(String name, long size, boolean directory, boolean regular, boolean link) {
            this(name, size, directory, regular, link, null);
        }
        public Entry(String name, long size, boolean directory, boolean regular, boolean link, Long modifiedMillis) {
            this.name = name; this.size = size; this.directory = directory; this.regular = regular; this.link = link;
            this.modifiedMillis = modifiedMillis;
        }
    }
    List<Entry> list(String path) throws IOException;
    Entry lstat(String path) throws IOException;
    void read(String path, OutputStream target) throws IOException;
    void write(String path, InputStream source) throws IOException;
    void mkdir(String path) throws IOException;
    void rename(String source, String target) throws IOException;
    void deleteTemporary(String path, boolean directory) throws IOException;
    String pathAssurance();
    default String capacity() { return "UNKNOWN"; }
    @Override void close() throws IOException;
}
