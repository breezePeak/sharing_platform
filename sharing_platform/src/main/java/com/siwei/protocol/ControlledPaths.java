package com.siwei.protocol;

import java.io.IOException;
import java.nio.file.*;

public final class ControlledPaths {
    private ControlledPaths() { }

    public static String remoteRoot(String value) {
        if (value == null || value.isBlank()) return null;
        if (!value.startsWith("/") || value.length() > 1000) throw invalid();
        if (value.equals("/")) return value;
        String normalized = value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
        relative(normalized.substring(1));
        return normalized;
    }

    public static String relative(String value) {
        if (value == null || value.isBlank() || value.startsWith("/") || value.contains("\\")
                || value.codePoints().anyMatch(c -> c < 32 || c == 127)) throw invalid();
        for (String part : value.split("/", -1)) {
            if (part.isEmpty() || part.equals(".") || part.equals("..") || part.startsWith("-")) throw invalid();
        }
        return value;
    }

    public static String resolve(String root, String relative) {
        if (root == null) throw invalid();
        return (root.equals("/") ? "" : remoteRoot(root)) + "/" + relative(relative);
    }

    public static boolean within(String root, String path) {
        return root != null && (root.equals("/") || path.equals(root) || path.startsWith(root + "/"));
    }

    public static Path localDirectory(String value) throws IOException {
        Path path = Paths.get(value);
        if (!path.isAbsolute()) throw invalid();
        rejectLinks(path);
        if (!Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) throw invalid();
        return path.toRealPath();
    }

    public static Path keyFile(String directory, String file) throws IOException {
        if (directory == null || directory.isBlank() || file == null || !file.matches("[a-zA-Z0-9][a-zA-Z0-9._-]{0,99}")) throw invalid();
        Path root = localDirectory(directory);
        Path path = root.resolve(file);
        rejectLinks(path);
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || !Files.isReadable(path)) throw invalid();
        return path;
    }

    private static void rejectLinks(Path path) throws IOException {
        Path current = path.getRoot();
        for (Path part : path) {
            current = current.resolve(part);
            java.nio.file.attribute.BasicFileAttributes attributes = Files.readAttributes(current,
                    java.nio.file.attribute.BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (attributes.isSymbolicLink() || attributes.isOther()) throw invalid();
        }
    }
    public static String keyDigest(Path path) throws IOException {
        try (java.io.InputStream input = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) {
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[8192]; int length; long count = 0;
            while ((length = input.read(buffer)) != -1) {
                count += length;
                if (count > 1024 * 1024) throw new IOException("KEY_FILE_TOO_LARGE");
                digest.update(buffer, 0, length);
            }
            StringBuilder hex = new StringBuilder();
            for (byte value : digest.digest()) hex.append(String.format("%02x", value & 255));
            return hex.toString();
        } catch (java.security.NoSuchAlgorithmException e) { throw new IllegalStateException("SHA256_UNAVAILABLE"); }
    }
    private static IllegalArgumentException invalid() { return new IllegalArgumentException("路径必须位于配置根目录内，且不能含链接或特殊路径段"); }
}
