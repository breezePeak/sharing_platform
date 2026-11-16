package com.siwei.protocol;

import java.io.*;
import java.util.List;
import java.util.concurrent.*;

/** A deadline measures inactivity during network operations, never total file/task duration. */
final class TimedRemoteFileClient implements RemoteFileClient {
    private final RemoteFileClient client;
    private final ScheduledExecutorService timer;
    private final long timeoutNanos;
    private final java.util.function.Consumer<RemoteFileClient> onClose;
    private volatile long progress;
    private boolean closed;

    TimedRemoteFileClient(RemoteFileClient client, ScheduledExecutorService timer, int seconds, java.util.function.Consumer<RemoteFileClient> onClose) {
        this.client = client; this.timer = timer; timeoutNanos = TimeUnit.SECONDS.toNanos(seconds);
        this.onClose = onClose;
    }
    <T> T io(Io<T> action) throws IOException {
        synchronized (this) { if (closed) throw new IOException("SESSION_CLOSED"); progress = System.nanoTime(); }
        boolean[] running = {true};
        ScheduledFuture<?> deadline = timer.scheduleWithFixedDelay(() -> {
            synchronized (this) {
                if (running[0] && !closed && System.nanoTime() - progress >= timeoutNanos) {
                    try { close(); } catch (IOException ignored) { }
                }
            }
        }, 100, 100, TimeUnit.MILLISECONDS);
        try {
            T result = action.run();
            synchronized (this) { if (closed) throw new IOException("NO_PROGRESS_TIMEOUT"); }
            return result;
        } finally { synchronized (this) { running[0] = false; deadline.cancel(false); } }
    }
    @Override public List<Entry> list(String path) throws IOException { return io(() -> client.list(path)); }
    @Override public Entry lstat(String path) throws IOException { return io(() -> client.lstat(path)); }
    @Override public void read(String path, OutputStream target) throws IOException {
        io(() -> {
            client.read(path, new FilterOutputStream(target) {
                @Override public void write(int value) throws IOException { out.write(value); progress = System.nanoTime(); }
                @Override public void write(byte[] bytes, int offset, int count) throws IOException {
                    out.write(bytes, offset, count); if (count > 0) progress = System.nanoTime();
                }
            });
            return null;
        });
    }
    @Override public void write(String path, InputStream source) throws IOException {
        io(() -> {
            client.write(path, new FilterInputStream(source) {
                @Override public int read() throws IOException {
                    int value = in.read(); if (value != -1) progress = System.nanoTime(); return value;
                }
                @Override public int read(byte[] bytes, int offset, int length) throws IOException {
                    int count = in.read(bytes, offset, length); if (count > 0) progress = System.nanoTime(); return count;
                }
            });
            return null;
        });
    }
    @Override public void mkdir(String path) throws IOException { io(() -> { client.mkdir(path); return null; }); }
    @Override public void rename(String source, String target) throws IOException { io(() -> { client.rename(source, target); return null; }); }
    @Override public void deleteTemporary(String path, boolean directory) throws IOException { io(() -> { client.deleteTemporary(path, directory); return null; }); }
    @Override public String capacity() {
        try { return io(client::capacity); } catch (IOException error) { return "UNKNOWN"; }
    }
    @Override public String pathAssurance() { return client.pathAssurance(); }
    @Override public synchronized void close() throws IOException {
        if (closed) return;
        closed = true;
        try { client.close(); } finally { onClose.accept(this); }
    }
    @FunctionalInterface interface Io<T> { T run() throws IOException; }
}
