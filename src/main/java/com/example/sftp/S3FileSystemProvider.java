package com.example.sftp;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.sync.ResponseTransformer;

import java.io.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.nio.file.spi.FileSystemProvider;
import java.util.*;

public class S3FileSystemProvider extends FileSystemProvider {
    private final S3Client s3Client;
    private final String bucketName;

    public S3FileSystemProvider(S3Client s3Client, String bucketName) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
    }

    @Override
    public String getScheme() {
        return "s3";
    }

    @Override
    public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        throw new UnsupportedOperationException("FileSystem creation through URI is not supported");

    }


    @Override
    public FileSystem getFileSystem(URI uri) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Path getPath(URI uri) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        String key = path.toString();
        if (options.contains(StandardOpenOption.READ)) {
            return new S3SeekableByteChannel(s3Client, bucketName, key);
        } else if (options.contains(StandardOpenOption.WRITE) || options.contains(StandardOpenOption.CREATE)) {
            return new S3OutputByteChannel(s3Client, bucketName, key);
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public FileChannel newFileChannel(Path path,
                                      Set<? extends OpenOption> options,
                                      FileAttribute<?>... attrs) throws IOException {
        // Convert OpenOptions to a readable format for debugging
        boolean write = options.contains(StandardOpenOption.WRITE);
        boolean read = options.contains(StandardOpenOption.READ);
        boolean create = options.contains(StandardOpenOption.CREATE);
        boolean createNew = options.contains(StandardOpenOption.CREATE_NEW);
        boolean append = options.contains(StandardOpenOption.APPEND);

        String key = path.toString();
        if (key.startsWith("/")) {
            key = key.substring(1);
        }

        // Create custom FileChannel implementation
        return new S3FileChannel(s3Client, bucketName, key, read, write, create, createNew, append);
    }

    private static class S3FileChannel extends FileChannel {
        private final S3Client s3Client;
        private final String bucket;
        private final String key;
        private final boolean read;
        private final boolean write;
        private final boolean create;
        private ByteBuffer buffer;
        private long position;
        private boolean closed;

        public S3FileChannel(S3Client s3Client, String bucket, String key,
                             boolean read, boolean write, boolean create,
                             boolean createNew, boolean append) throws IOException {
            this.s3Client = s3Client;
            this.bucket = bucket;
            this.key = key;
            this.read = read;
            this.write = write;
            this.create = create;
            this.position = 0;
            this.closed = false;

            if (read) {
                // For read operations, load the content into buffer
                try {
                    byte[] data = s3Client.getObject(GetObjectRequest.builder()
                                    .bucket(bucket)
                                    .key(key)
                                    .build(),
                            ResponseTransformer.toBytes()).asByteArray();

                    buffer = ByteBuffer.wrap(data);
                } catch (NoSuchKeyException e) {
                    if (!create) {
                        throw new NoSuchFileException(key);
                    }
                    buffer = ByteBuffer.allocate(0);
                }
            } else {
                buffer = ByteBuffer.allocate(8192); // Initial buffer size
            }
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            if (!read) throw new NonReadableChannelException();
            if (closed) throw new ClosedChannelException();

            buffer.position((int) position);
            if (buffer.remaining() == 0) return -1;

            int bytesRead = Math.min(dst.remaining(), buffer.remaining());
            byte[] data = new byte[bytesRead];
            buffer.get(data);
            dst.put(data);
            position += bytesRead;
            return bytesRead;
        }

        @Override
        public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
            return 0;
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            if (!write) throw new NonWritableChannelException();
            if (closed) throw new ClosedChannelException();

            int bytesWritten = src.remaining();
            byte[] data = new byte[bytesWritten];
            src.get(data);

            // Ensure buffer has enough capacity
            if (buffer.remaining() < bytesWritten) {
                ByteBuffer newBuffer = ByteBuffer.allocate(buffer.capacity() * 2);
                buffer.flip();
                newBuffer.put(buffer);
                buffer = newBuffer;
            }

            buffer.put(data);
            position += bytesWritten;
            return bytesWritten;
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
            return 0;
        }

        @Override
        public long position() throws IOException {
            if (closed) throw new ClosedChannelException();
            return position;
        }

        @Override
        public FileChannel position(long newPosition) throws IOException {
            if (closed) throw new ClosedChannelException();
            if (newPosition < 0) throw new IllegalArgumentException("Negative position");
            this.position = newPosition;
            return this;
        }

        @Override
        public long size() throws IOException {
            if (closed) throw new ClosedChannelException();
            return buffer.capacity();
        }

        @Override
        public FileChannel truncate(long size) throws IOException {
            if (!write) throw new NonWritableChannelException();
            if (closed) throw new ClosedChannelException();

            ByteBuffer newBuffer = ByteBuffer.allocate((int) size);
            buffer.flip();
            if (buffer.remaining() > size) {
                byte[] data = new byte[(int) size];
                buffer.get(data);
                newBuffer.put(data);
            } else {
                newBuffer.put(buffer);
            }
            buffer = newBuffer;
            position = Math.min(position, size);
            return this;
        }

        @Override
        public void force(boolean metaData) throws IOException {
            if (closed) throw new ClosedChannelException();
            if (write) {
                // Upload to S3
                buffer.flip();
                byte[] data = new byte[buffer.remaining()];
                buffer.get(data);

                s3Client.putObject(PutObjectRequest.builder()
                                .bucket(bucket)
                                .key(key)
                                .build(),
                        RequestBody.fromBytes(data));
            }
        }

        @Override
        public long transferTo(long position, long count, WritableByteChannel target)
                throws IOException {
            throw new UnsupportedOperationException("Transfer operations not supported");
        }

        @Override
        public long transferFrom(ReadableByteChannel src, long position, long count)
                throws IOException {
            throw new UnsupportedOperationException("Transfer operations not supported");
        }

        @Override
        public int read(ByteBuffer dst, long position) throws IOException {
            throw new UnsupportedOperationException("Positioned read not supported");
        }

        @Override
        public int write(ByteBuffer src, long position) throws IOException {
            throw new UnsupportedOperationException("Positioned write not supported");
        }

        @Override
        public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
            return null;
        }

        @Override
        public FileLock lock(long position, long size, boolean shared) throws IOException {
            return null;
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared) throws IOException {
            return null;
        }

        @Override
        protected void implCloseChannel() throws IOException {
            if (write) {
                force(false); // Upload any remaining data
            }
            closed = true;
        }
    }

    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        String key = dir.toString();
        System.out.println("\n=== Create Directory Debug ===");
        System.out.println("Original path: " + key);

        // Normalize the key
        if (key.startsWith("/")) {
            key = key.substring(1);
        }
        if (!key.endsWith("/")) {
            key += "/";
        }

        System.out.println("Normalized key: " + key);

        try {
            // First check if directory already exists

            if (directoryExists(key)) {
                System.out.println("Directory already exists: " + key);
                throw new FileAlreadyExistsException(dir.toString());
            }

            // Create directory marker
            System.out.println("Creating directory marker: " + key);

            try {
                // Check direct match
                HeadObjectRequest headRequest = HeadObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .build();

                try {
                    s3Client.headObject(headRequest);
                    System.out.println("Directory marker already exists: " + key);
                    throw new FileAlreadyExistsException(dir.toString());
                } catch (NoSuchKeyException e) {
                    // Key doesn't exist, which is what we want
                    System.out.println("Directory marker doesn't exist, proceeding with creation");
                }

                // Also check if there are any objects with this prefix
                ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                        .bucket(bucketName)
                        .prefix(key)
                        .maxKeys(1)
                        .build();

                ListObjectsV2Response response = s3Client.listObjectsV2(listRequest);
                if (response.hasContents()) {
                    System.out.println("Objects exist with prefix: " + key);
                    throw new FileAlreadyExistsException(dir.toString());
                }

                // Create directory marker
                System.out.println("Creating directory marker: " + key);
                PutObjectRequest request = PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .contentLength(0L)
                        .build();

                s3Client.putObject(request, RequestBody.empty());
                System.out.println("Successfully created directory marker");

            } catch (S3Exception e) {
                if (e instanceof NoSuchKeyException) {
                    throw new FileNotFoundException(dir.toString());
                }
                System.out.println("S3 error during directory creation: " + e.getMessage());
                throw new IOException("Failed to create directory: " + dir, e);
            }
        } catch (Exception e) {
            if (!(e instanceof FileAlreadyExistsException)) {
                System.out.println("Unexpected error: " + e.getMessage());
                throw new IOException("Failed to create directory: " + dir, e);
            }
            throw e;
        }
    }

    private boolean directoryExists(String key) {
        System.out.println("Checking if directory exists: " + key);

        try {
            // Ensure key ends with / for directory check
            if (!key.endsWith("/")) {
                key += "/";
            }

            // Method 1: Check for directory marker
            HeadObjectRequest request = HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            try {
                s3Client.headObject(request);
                System.out.println("Found directory marker");
                return true;
            } catch (NoSuchKeyException e) {
                // Method 2: Check for any objects with this prefix
                ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                        .bucket(bucketName)
                        .prefix(key)
                        .maxKeys(1)
                        .build();

                ListObjectsV2Response response = s3Client.listObjectsV2(listRequest);
                boolean hasContents = response.hasContents();
                System.out.println("Prefix check result: " + (hasContents ? "directory exists" : "directory does not exist"));
                return hasContents;
            }
        } catch (S3Exception e) {
            System.out.println("S3 error while checking directory existence: " + e.getMessage());
            return false;
        }
    }


    @Override
    public void delete(Path path) throws IOException {
        try {
            String key = path.toString();
            if (key.startsWith("/")) {
                key = key.substring(1);
            }

            // Check if it's a directory
            if (exists(path) && isDirectory(path)) {
                // List and delete all objects under this prefix
                ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                        .bucket(bucketName)
                        .prefix(key.endsWith("/") ? key : key + "/")
                        .build();

                ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);
                for (S3Object s3Object : listResponse.contents()) {
                    DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                            .bucket(bucketName)
                            .key(s3Object.key())
                            .build();
                    s3Client.deleteObject(deleteRequest);
                }
            }

            // Delete the object/directory marker
            DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();
            s3Client.deleteObject(deleteRequest);
        } catch (S3Exception e) {
            throw new IOException("Failed to delete: " + path, e);
        }
    }

    @Override
    public void copy(Path source, Path target, CopyOption... options) throws IOException {
        try {
            String sourceKey = source.toString();
            String targetKey = target.toString();
            if (sourceKey.startsWith("/")) sourceKey = sourceKey.substring(1);
            if (targetKey.startsWith("/")) targetKey = targetKey.substring(1);

            CopyObjectRequest request = CopyObjectRequest.builder()
                    .sourceBucket(bucketName)
                    .sourceKey(sourceKey)
                    .destinationBucket(bucketName)
                    .destinationKey(targetKey)
                    .build();

            s3Client.copyObject(request);
        } catch (S3Exception e) {
            throw new IOException("Failed to copy from " + source + " to " + target, e);
        }
    }

    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {
        copy(source, target, options);
        delete(source);
    }

    @Override
    public boolean isSameFile(Path path1, Path path2) {
        return path1.equals(path2);
    }

    @Override
    public boolean isHidden(Path path) {
        return false;
    }

    @Override
    public FileStore getFileStore(Path path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkAccess(Path path, AccessMode... modes) throws IOException {
        String key = path.toString();
        if (key.startsWith("/")) {
            key = key.substring(1);
        }

        System.out.println("\n=== CheckAccess Debug ===");
        System.out.println("Original path: " + path);
        System.out.println("Normalized key: " + key);
        System.out.println("Bucket: " + bucketName);

        try {
            // First check if it's a directory marker
            String dirKey = key.endsWith("/") ? key : key + "/";
            try {
                ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                        .bucket(bucketName)
                        .prefix(dirKey)
                        .maxKeys(1)
                        .build();

                ListObjectsV2Response response = s3Client.listObjectsV2(listRequest);
                System.out.println("Directory check - Has contents: " + response.hasContents());
                if (response.hasContents()) {
                    return; // Directory exists
                }
            } catch (Exception e) {
                System.out.println("Directory check failed: " + e.getMessage());
            }

            // Then check as a file
            try {
                HeadObjectRequest request = HeadObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .build();
                s3Client.headObject(request);
                System.out.println("File exists: " + key);
                return;
            } catch (NoSuchKeyException e) {
                System.out.println("File does not exist: " + key);
                // Only throw if both file and directory checks failed
                throw new NoSuchFileException(path.toString());
            }
        } catch (S3Exception e) {
            System.err.println("S3 error: " + e.getMessage());
            throw new IOException("Failed to check access: " + path, e);
        }
    }


    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        return null;
    }


    @Override
    public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options)
            throws IOException {
        if (type != BasicFileAttributes.class && type != PosixFileAttributes.class) {
            throw new UnsupportedOperationException("Only BasicFileAttributes and PosixFileAttributes are supported");
        }

        String key = path.toString();
        System.out.println("\n=== Reading Attributes ===");
        System.out.println("Path: " + path);
        System.out.println("Original key: " + key);

        // Handle relative paths by prepending current directory
        if (!key.startsWith("/")) {
            String currentDir = "home/admin/";  // You might want to make this configurable
            key = currentDir + key;
            System.out.println("Adjusted key for relative path: " + key);
        } else if (key.startsWith("/")) {
            key = key.substring(1);
            System.out.println("Removed leading slash: " + key);
        }

        try {
            HeadObjectResponse response = null;
            boolean isDirectory = false;

            try {
                // Try as a file first
                HeadObjectRequest request = HeadObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .build();
                System.out.println("Checking file at key: " + key);
                response = s3Client.headObject(request);
                System.out.println("Found file object");

            } catch (NoSuchKeyException e) {
                // If not found as file, try as directory
                String dirKey = key.endsWith("/") ? key : key + "/";
                System.out.println("File not found, checking directory at: " + dirKey);

                try {
                    ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                            .bucket(bucketName)
                            .prefix(dirKey)
                            .maxKeys(1)
                            .build();

                    ListObjectsV2Response listing = s3Client.listObjectsV2(listRequest);
                    if (listing.hasContents()) {
                        isDirectory = true;
                        System.out.println("Found as directory");
                    } else {
                        System.out.println("Not found as directory either");
                        throw new NoSuchFileException(path.toString());
                    }
                } catch (S3Exception s3e) {
                    System.out.println("S3 error checking directory: " + s3e.getMessage());
                    throw new NoSuchFileException(path.toString());
                }
            }

            System.out.println("Creating attributes - isDirectory: " + isDirectory);
            return (A) new S3FileAttributes(response, key, isDirectory);

        } catch (S3Exception e) {
            System.out.println("S3 error: " + e.getMessage());
            throw new IOException("Failed to read attributes: " + path, e);
        }
    }

    @Override
    public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options)
            throws IOException {
        System.out.println("\n=== Reading Attributes Map ===");
        System.out.println("Path: " + path);
        System.out.println("Requested attributes: " + attributes);

        BasicFileAttributes attr = readAttributes(path, BasicFileAttributes.class, options);
        Map<String, Object> map = new HashMap<>();

        // Basic attributes
        map.put("lastModifiedTime", attr.lastModifiedTime());
        map.put("lastAccessTime", attr.lastAccessTime());
        map.put("creationTime", attr.creationTime());
        map.put("size", attr.size());
        map.put("isRegularFile", attr.isRegularFile());
        map.put("isDirectory", attr.isDirectory());
        map.put("isSymbolicLink", attr.isSymbolicLink());
        map.put("isOther", attr.isOther());
        map.put("fileKey", attr.fileKey());

        // Add POSIX attributes
        map.put("permissions", getPosixPermissions(attr.isDirectory()));
        map.put("owner", "admin");
        map.put("group", "admin");

        System.out.println("Returning attributes map: " + map);
        return map;
    }

    private Set<PosixFilePermission> getPosixPermissions(boolean isDirectory) {
        Set<PosixFilePermission> permissions = new HashSet<>();

        // Owner permissions
        permissions.add(PosixFilePermission.OWNER_READ);
        permissions.add(PosixFilePermission.OWNER_WRITE);
        if (isDirectory) {
            permissions.add(PosixFilePermission.OWNER_EXECUTE);
        }

        // Group permissions
        permissions.add(PosixFilePermission.GROUP_READ);
        if (isDirectory) {
            permissions.add(PosixFilePermission.GROUP_EXECUTE);
        }

        // Others permissions
        permissions.add(PosixFilePermission.OTHERS_READ);
        if (isDirectory) {
            permissions.add(PosixFilePermission.OTHERS_EXECUTE);
        }

        return permissions;
    }

    @Override
    public void setAttribute(Path path, String attribute, Object value, LinkOption... options) {
        throw new UnsupportedOperationException();
    }

    private boolean exists(Path path) {
        try {
            checkAccess(path);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private boolean isDirectory(Path path) {
        String key = path.toString();
        if (key.startsWith("/")) {
            key = key.substring(1);
        }

        if (!key.endsWith("/")) {
            key += "/";
        }

        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(key)
                .maxKeys(1)
                .build();

        ListObjectsV2Response response = s3Client.listObjectsV2(request);
        return response.hasContents();
    }

    private String normalizeKey(String key) {
        // Remove leading slash
        if (key.startsWith("/")) {
            key = key.substring(1);
        }

        // Ensure trailing slash for directories
        if (!key.isEmpty() && !key.endsWith("/")) {
            key += "/";
        }

        return key;
    }

    private boolean isDirectory(String key) {
        try {
            // Check if it's the root directory
            if (key.isEmpty() || key.equals("/")) {
                return true;
            }

            // Try to get the directory marker object
            try {
                HeadObjectRequest request = HeadObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .build();
                s3Client.headObject(request);
                return true;
            } catch (NoSuchKeyException e) {
                // If directory marker doesn't exist, check if there are any objects with this prefix
                ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                        .bucket(bucketName)
                        .prefix(key)
                        .maxKeys(1)
                        .build();

                ListObjectsV2Response response = s3Client.listObjectsV2(listRequest);
                return response.hasContents();
            }
        } catch (S3Exception e) {
            return false;
        }
    }

    public void listS3Contents() {
        System.out.println("\n=== Listing All S3 Contents ===");
        try {
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .build();

            ListObjectsV2Response response = s3Client.listObjectsV2(request);
            System.out.println("Total objects: " + response.keyCount());

            if (response.contents() != null) {
                response.contents().forEach(obj -> {
                    System.out.println("\nObject:");
                    System.out.println("Key: " + obj.key());
                    System.out.println("Size: " + obj.size());
                    System.out.println("Last Modified: " + obj.lastModified());
                    System.out.println("Storage Class: " + obj.storageClass());
                });
            }
        } catch (S3Exception e) {
            System.err.println("Failed to list bucket contents: " + e.getMessage());
        }
    }


    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        String prefix = dir.toString();
        if (prefix.startsWith("/")) {
            prefix = prefix.substring(1);
        }
        if (!prefix.endsWith("/")) {
            prefix += "/";
        }

        System.out.println("\n=== Directory Listing Debug ===");
        System.out.println("Current directory: " + dir);
        System.out.println("S3 prefix: " + prefix);

        try {
            List<Path> paths = new ArrayList<>();
            String continuationToken = null;

            do {
                ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                        .bucket(bucketName)
                        .prefix(prefix)
                        .delimiter("/");

                if (continuationToken != null) {
                    requestBuilder.continuationToken(continuationToken);
                }

                ListObjectsV2Response listing = s3Client.listObjectsV2(requestBuilder.build());

                // Process files with debug logging
                if (listing.contents() != null) {
                    for (S3Object s3Object : listing.contents()) {
                        String key = s3Object.key();
                        System.out.println("\nProcessing S3 Object:");
                        System.out.println("Found object key: " + key);

                        // Skip directory markers
                        if (!key.equals(prefix)) {
                            // Extract relative path
                            String relativeName = key.substring(prefix.length());
                            System.out.println("Extracted relative name: " + relativeName);

                            if (!relativeName.isEmpty()) {
                                S3FileSystem fs = (S3FileSystem) dir.getFileSystem();
                                Path filePath = fs.getPath(relativeName);

                                // Debug the created path
                                System.out.println("\nPath Debug Information:");
                                ((S3Path) filePath).debugPath();

                                try {
                                    if (filter.accept(filePath)) {
                                        paths.add(filePath);
                                        System.out.println("Added to results: " + filePath);
                                        System.out.println("Path toString(): " + filePath.toString());
                                        System.out.println("Path class: " + filePath.getClass().getName());
                                    } else {
                                        System.out.println("Path filtered out: " + filePath);
                                    }
                                } catch (IOException e) {
                                    System.out.println("Filter error for " + filePath + ": " + e.getMessage());
                                }
                            } else {
                                System.out.println("Skipped empty relative name");
                            }
                        } else {
                            System.out.println("Skipped directory marker: " + key);
                        }
                    }
                }

                // Process directories (common prefixes)
                if (listing.commonPrefixes() != null) {
                    for (CommonPrefix commonPrefix : listing.commonPrefixes()) {
                        String prefixKey = commonPrefix.prefix();
                        System.out.println("\nProcessing Directory:");
                        System.out.println("Found prefix: " + prefixKey);

                        // Extract relative directory name
                        String relativeName = prefixKey.substring(prefix.length());
                        if (relativeName.endsWith("/")) {
                            relativeName = relativeName.substring(0, relativeName.length() - 1);
                        }
                        System.out.println("Extracted relative name: " + relativeName);

                        if (!relativeName.isEmpty()) {
                            S3FileSystem fs = (S3FileSystem) dir.getFileSystem();
                            Path dirPath = fs.getPath(relativeName);

                            // Debug the created directory path
                            System.out.println("\nDirectory Path Debug Information:");
                            ((S3Path) dirPath).debugPath();

                            try {
                                if (filter.accept(dirPath)) {
                                    paths.add(dirPath);
                                    System.out.println("Added to results: " + dirPath);
                                    System.out.println("Path toString(): " + dirPath.toString());
                                    System.out.println("Path class: " + dirPath.getClass().getName());
                                } else {
                                    System.out.println("Directory path filtered out: " + dirPath);
                                }
                            } catch (IOException e) {
                                System.out.println("Filter error for directory " + dirPath + ": " + e.getMessage());
                            }
                        }
                    }
                }

                continuationToken = listing.isTruncated() ? listing.nextContinuationToken() : null;

            } while (continuationToken != null);

            System.out.println("\n=== Final Results ===");
            System.out.println("Total paths found: " + paths.size());
            paths.forEach(p -> {
                System.out.println("- Path: " + p);
                System.out.println("  Class: " + p.getClass().getName());
                System.out.println("  ToString: " + p.toString());
                if (p instanceof S3Path) {
                    System.out.println("  Internal path: " + ((S3Path)p).getPathAsString());
                }
            });

            System.out.println("\nCreating DirectoryStream with paths:");
            paths.forEach(p -> System.out.println("Path to be included: " + p + ", Class: " + p.getClass().getName()));
            return new S3DirectoryStream(paths);

        } catch (S3Exception e) {
            System.err.println("S3 error: " + e.getMessage());
            throw new IOException("Failed to list directory: " + dir, e);
        }
    }

    private static class S3DirectoryStream implements DirectoryStream<Path> {
        private final List<Path> paths;
        private boolean closed;
        private boolean iteratorReturned;

        public S3DirectoryStream(List<Path> paths) {
            this.paths = Collections.unmodifiableList(new ArrayList<>(paths));
            this.closed = false;
            this.iteratorReturned = false;
        }

        @Override
        public Iterator<Path> iterator() {
            if (closed) {
                throw new IllegalStateException("Directory stream is closed");
            }
            if (iteratorReturned) {
                throw new IllegalStateException("Iterator has already been returned");
            }

            iteratorReturned = true;

            return new Iterator<Path>() {
                private final Iterator<Path> it = paths.iterator();

                @Override
                public boolean hasNext() {
                    if (closed) {
                        throw new IllegalStateException("Directory stream is closed");
                    }
                    return it.hasNext();
                }

                @Override
                public Path next() {
                    if (closed) {
                        throw new IllegalStateException("Directory stream is closed");
                    }
                    Path nextPath = it.next();
                    System.out.println("Returning next path from iterator: " + nextPath);
                    return nextPath;
                }
            };
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
