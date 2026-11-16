package com.siwei;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;

/** Real loopback FTP wire fixture: control disconnect deliberately does not stop an active data transfer. */
public final class StreamingFtpServer implements AutoCloseable {
    public enum Fault { RETR, STOR, LIST, OPENING, ACTIVE_ACCEPT, WELCOME, AUTH }
    private final ServerSocket listener = new ServerSocket(0, 10, InetAddress.getByName("127.0.0.1"));
    private final Set<Closeable> resources = ConcurrentHashMap.newKeySet();
    private final ExecutorService handlers = Executors.newCachedThreadPool(r -> { Thread t = new Thread(r, "ftp-wire-fixture"); t.setDaemon(true); return t; });
    private final ScheduledExecutorService clock = Executors.newSingleThreadScheduledExecutor();
    private final Semaphore ticks = new Semaphore(0);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Fault fault;
    public final AtomicInteger started = new AtomicInteger(), stopped = new AtomicInteger(), connections = new AtomicInteger();
    public final AtomicLong bytes = new AtomicLong();
    public final CountDownLatch opening = new CountDownLatch(1), releaseOpening = new CountDownLatch(1);
    public final CompletableFuture<Void> openingDataClosed = new CompletableFuture<>();
    public final ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();

    public StreamingFtpServer(Fault fault) throws IOException {
        this.fault = fault;
        clock.scheduleAtFixedRate(ticks::release, 0, 20, TimeUnit.MILLISECONDS);
        handlers.execute(() -> {
            try {
                while (!closed.get()) {
                    Socket control = listener.accept(); resources.add(control);
                    handlers.execute(() -> serve(control));
                }
            } catch (IOException e) { if (!closed.get()) failures.add(e); }
        });
    }
    public int port() { return listener.getLocalPort(); }

    private void serve(Socket control) {
        ServerSocket passive = null;
        InetSocketAddress active = null;
        boolean slowHandshake = connections.incrementAndGet() <= 2;
        try (Socket connection = control;
             BufferedReader input = new BufferedReader(new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter output = new PrintWriter(new OutputStreamWriter(connection.getOutputStream(), StandardCharsets.UTF_8), true)) {
            connection.setSoTimeout(15000);
            if (fault == Fault.WELCOME && slowHandshake) { streamReply(output, "220-"); return; }
            reply(output, "220 streaming test fixture");
            String line;
            while (!closed.get() && (line = input.readLine()) != null) {
                String[] parts = line.split(" ", 2);
                String command = parts[0], path = parts.length == 2 ? parts[1] : "";
                switch (command) {
                    case "USER": reply(output, "331 password"); break;
                    case "PASS":
                        if (fault == Fault.AUTH && slowHandshake) { streamReply(output, "230-"); return; }
                        reply(output, "230 logged in"); break;
                    case "TYPE": reply(output, "200 binary"); break;
                    case "SYST": reply(output, "215 UNIX Type: L8"); break;
                    case "MLST":
                        boolean file = path.endsWith(".bin");
                        reply(output, "250-Listing\r\n type=" + (file ? "file;size=1000000000" : "dir") + "; " + path + "\r\n250 End"); break;
                    case "PASV":
                        if (passive != null) { passive.close(); resources.remove(passive); }
                        passive = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1")); resources.add(passive);
                        reply(output, "227 Entering Passive Mode (127,0,0,1," + passive.getLocalPort() / 256 + "," + passive.getLocalPort() % 256 + ")"); break;
                    case "PORT":
                        String[] address = path.split(",");
                        if (!path.startsWith("127,0,0,1,")) throw new IOException("Non-loopback active address");
                        active = new InetSocketAddress("127.0.0.1", Integer.parseInt(address[4]) * 256 + Integer.parseInt(address[5]));
                        reply(output, "200 active address accepted"); break;
                    case "LIST": case "RETR": case "STOR":
                        if (fault == Fault.ACTIVE_ACCEPT) {
                            reply(output, "150 waiting for active connection"); opening.countDown();
                            if (!releaseOpening.await(10, TimeUnit.SECONDS)) throw new IOException("Active accept test deadline");
                            break;
                        }
                        Socket data;
                        if (passive != null) { passive.setSoTimeout(5000); data = passive.accept(); }
                        else {
                            data = new Socket(); data.connect(active, 5000);
                        }
                        resources.add(data);
                        try (Socket channel = data) {
                            if (fault == Fault.OPENING && command.equals("RETR")) {
                                handlers.execute(() -> {
                                    try {
                                        if (channel.getInputStream().read() == -1) openingDataClosed.complete(null);
                                    } catch (IOException e) { openingDataClosed.complete(null); }
                                });
                                opening.countDown();
                                if (!releaseOpening.await(10, TimeUnit.SECONDS)) throw new IOException("Opening test deadline");
                            }
                            reply(output, "150 data transfer");
                            if (command.equals(fault.name())) stream(channel, command);
                            else channel.getOutputStream().write("-rw-r--r-- 1 owner group 1 Jan 01 2026 sample.bin\r\n".getBytes(StandardCharsets.UTF_8));
                        } finally { resources.remove(data); }
                        reply(output, "226 completed"); break;
                    default: reply(output, "200 ok");
                }
            }
        } catch (IOException e) {
            // Cancellation closes either channel; transfer completion is observed separately below.
        } catch (InterruptedException e) { Thread.currentThread().interrupt();
        } catch (RuntimeException e) { failures.add(e);
        } finally {
            resources.remove(control);
            if (passive != null) { try { passive.close(); } catch (IOException ignored) { } resources.remove(passive); }
        }
    }

    private void streamReply(PrintWriter output, String prefix) throws InterruptedException {
        started.incrementAndGet();
        try {
            while (!closed.get()) {
                if (!ticks.tryAcquire(1, TimeUnit.SECONDS)) break;
                reply(output, prefix + " waiting");
                if (output.checkError()) break;
            }
        } finally { stopped.incrementAndGet(); }
    }

    private void stream(Socket data, String command) throws IOException, InterruptedException {
        started.incrementAndGet();
        try {
            byte[] chunk = command.equals("LIST") ? "-rw-r--r-- 1 owner group 1 Jan 01 2026 file.bin\r\n".getBytes(StandardCharsets.UTF_8) : new byte[8192];
            while (!closed.get()) {
                if (!ticks.tryAcquire(1, TimeUnit.SECONDS)) throw new IOException("Fixture pacing deadline");
                if (command.equals("STOR")) {
                    int read = data.getInputStream().read(chunk);
                    if (read < 0) break;
                    bytes.addAndGet(read);
                } else { data.getOutputStream().write(chunk); data.getOutputStream().flush(); bytes.addAndGet(chunk.length); }
            }
        } finally { stopped.incrementAndGet(); }
    }
    private static void reply(PrintWriter output, String value) { output.print(value + "\r\n"); output.flush(); }
    public static void await(BooleanSupplier condition, Duration timeout) throws Exception {
        long end = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= end) throw new AssertionError("Timed out waiting for FTP fixture condition");
            Thread.sleep(10);
        }
    }
    @Override public void close() throws Exception {
        closed.set(true); releaseOpening.countDown(); listener.close();
        for (Closeable resource : resources) try { resource.close(); } catch (IOException ignored) { }
        clock.shutdownNow(); handlers.shutdownNow();
        if (!handlers.awaitTermination(3, TimeUnit.SECONDS)) throw new AssertionError("FTP fixture did not stop");
    }
}
