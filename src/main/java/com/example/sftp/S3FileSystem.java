package com.example.sftp;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.nio.file.*;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Collections;
import java.util.Set;

public class S3FileSystem extends FileSystem {
    private final S3Client s3Client;
    private final String bucketName;
    private final S3FileSystemProvider provider;
    private final Path rootDirectory;
    private boolean isOpen;

    public S3FileSystem(S3Client s3Client, String bucketName) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.provider = new S3FileSystemProvider(s3Client, bucketName);
        this.rootDirectory = new S3Path(this, "/");
        this.isOpen = true;
    }

    @Override
    public FileSystemProvider provider() {
        return provider;
    }

    @Override
    public void close() {
        System.out.println("S3FileSystem.close() called from: " +
                Thread.currentThread().getStackTrace()[2].getClassName() + "." +
                Thread.currentThread().getStackTrace()[2].getMethodName() +
                "() line: " + Thread.currentThread().getStackTrace()[2].getLineNumber());

        // Do not close the S3Client here as it's managed by the factory
        System.out.println("S3FileSystem.close() - NOT closing S3Client");
    }



    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public String getSeparator() {
        return "/";
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        return Collections.singletonList(new S3Path(this, "/"));
    }

    @Override
    public Iterable<FileStore> getFileStores() {
        return Collections.emptyList();
    }

    @Override
    public Set<String> supportedFileAttributeViews() {
        return Set.of("basic");
    }

    @Override
    public Path getPath(String first, String... more) {
        System.out.println("GetPath called with: " + first);

        // Handle empty path
        if (first == null || first.isEmpty()) {
            return new S3Path(this, "/");
        }

        // For relative paths, don't add leading slash
        if (!first.startsWith("/")) {
            System.out.println("Creating relative path: " + first);
            return new S3Path(this, first);
        }

        // For absolute paths, preserve the leading slash
        System.out.println("Creating absolute path: " + first);
        return new S3Path(this, first);
    }

    @Override
    public PathMatcher getPathMatcher(String syntaxAndPattern) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchService newWatchService() {
        throw new UnsupportedOperationException();
    }

    public S3Client getS3Client() {
        return s3Client;
    }

    public String getBucketName() {
        return bucketName;
    }

    public Path getRootDirectory() {
        return rootDirectory;
    }
}
