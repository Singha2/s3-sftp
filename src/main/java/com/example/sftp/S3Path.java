package com.example.sftp;

import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.*;

public class S3Path implements Path {
    private final S3FileSystem fileSystem;
    private final String path;
    private final boolean isAbsolute;

    public S3Path(S3FileSystem fileSystem, String path) {
        this.fileSystem = Objects.requireNonNull(fileSystem, "FileSystem cannot be null");
        this.isAbsolute = path.startsWith("/");

        // Store path without leading slash for relative paths
        if (isAbsolute) {
            this.path = path;
        } else {
            this.path = path.startsWith("/") ? path.substring(1) : path;
        }

        System.out.println("S3Path created - Original: " + path + ", Stored: " + this.path + ", isAbsolute: " + this.isAbsolute);
    }


    @Override
    public FileSystem getFileSystem() {
        return fileSystem;
    }

    @Override
    public boolean isAbsolute() {
        return isAbsolute;
    }

    @Override
    public Path getRoot() {
        return new S3Path(fileSystem, "/");
    }

    @Override
    public Path getFileName() {
        if (path.equals("/")) {
            return null;
        }
        String name = path;
        if (name.endsWith("/")) {
            name = name.substring(0, name.length() - 1);
        }
        int lastSeparator = name.lastIndexOf("/");
        return lastSeparator == -1 ? this :
                new S3Path(fileSystem, name.substring(lastSeparator + 1));
    }

    @Override
    public Path getParent() {
        if (path.equals("/") || !path.contains("/")) {
            return null;
        }
        String parent = path;
        if (parent.endsWith("/")) {
            parent = parent.substring(0, parent.length() - 1);
        }
        int lastSeparator = parent.lastIndexOf("/");
        if (lastSeparator <= 0) {
            return new S3Path(fileSystem, "/");
        }
        return new S3Path(fileSystem, parent.substring(0, lastSeparator));
    }

    @Override
    public int getNameCount() {
        if (path.equals("/")) {
            return 0;
        }
        return path.substring(1).split("/").length;
    }

    @Override
    public Path getName(int index) {
        if (index < 0) {
            throw new IllegalArgumentException();
        }
        String[] components = path.substring(1).split("/");
        if (index >= components.length) {
            throw new IllegalArgumentException();
        }
        return new S3Path(fileSystem, components[index]);
    }

    @Override
    public Path subpath(int beginIndex, int endIndex) {
        if (beginIndex < 0 || endIndex > getNameCount() || beginIndex >= endIndex) {
            throw new IllegalArgumentException();
        }
        String[] components = path.substring(1).split("/");
        String subpath = String.join("/", Arrays.copyOfRange(components, beginIndex, endIndex));
        return new S3Path(fileSystem, subpath);
    }

    @Override
    public boolean startsWith(Path other) {
        if (!(other instanceof S3Path)) {
            return false;
        }
        return path.startsWith(other.toString());
    }

    @Override
    public boolean endsWith(Path other) {
        if (!(other instanceof S3Path)) {
            return false;
        }
        return path.endsWith(other.toString());
    }

    @Override
    public Path normalize() {
        // Already normalized in constructor
        return this;
    }

    @Override
    public Path resolve(String other) {
        if (other == null || other.isEmpty()) {
            return this;
        }
        if (other.startsWith("/")) {
            return new S3Path(fileSystem, other);
        }
        String resolved = path;
        if (!resolved.endsWith("/")) {
            resolved += "/";
        }
        resolved += other;
        return new S3Path(fileSystem, resolved);
    }



    public String getPathAsString() {
        return path;
    }

    @Override
    public Path resolve(Path other) {
        if (other.isAbsolute()) {
            return other;
        }
        if (other.toString().isEmpty()) {
            return this;
        }
        String resolved = path;
        if (!resolved.endsWith("/")) {
            resolved += "/";
        }
        resolved += other.toString();
        return new S3Path(fileSystem, resolved);
    }

    @Override
    public Path relativize(Path other) {
        throw new UnsupportedOperationException();
    }

    @Override
    public URI toUri() {
        return URI.create("s3://" + fileSystem.getBucketName() + path);
    }

    @Override
    public Path toAbsolutePath() {
        return isAbsolute() ? this : new S3Path(fileSystem, "/" + path);
    }


    @Override
    public Path toRealPath(LinkOption... options) throws IOException {
        return toAbsolutePath();
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Path> iterator() {
        if (path.equals("/")) {
            return Collections.emptyIterator();
        }
        List<Path> components = new ArrayList<>();
        String[] parts = path.substring(1).split("/");
        for (String part : parts) {
            components.add(new S3Path(fileSystem, part));
        }
        return components.iterator();
    }

    @Override
    public int compareTo(Path other) {
        return path.compareTo(other.toString());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof S3Path)) return false;
        S3Path other = (S3Path) o;
        return Objects.equals(fileSystem, other.fileSystem) &&
                Objects.equals(path, other.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileSystem, path);
    }

    @Override
    public String toString() {
        // For relative paths, don't include leading slash
        if (!isAbsolute && path.startsWith("/")) {
            return path.substring(1);
        }
        return path;
    }

    public void debugPath() {
        System.out.println("S3Path Debug:");
        System.out.println("- Original path: " + path);
        System.out.println("- Is absolute: " + isAbsolute);
        System.out.println("- toString(): " + toString());
        System.out.println("- getFileName(): " + getFileName());
    }
}