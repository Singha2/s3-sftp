package com.example.sftp;

import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Instant;

public class S3FileAttributes implements BasicFileAttributes {
    private final HeadObjectResponse objectResponse;
    private final String path;
    private final boolean isDirectory;

    public S3FileAttributes(HeadObjectResponse objectResponse, String path, boolean isDirectory) {
        this.objectResponse = objectResponse;
        this.path = path;
        this.isDirectory = isDirectory;
    }

    @Override
    public FileTime lastModifiedTime() {
        if (objectResponse != null) {
            return FileTime.from(objectResponse.lastModified());
        }
        return FileTime.from(Instant.now()); // Default for directories
    }

    @Override
    public FileTime lastAccessTime() {
        return lastModifiedTime();
    }

    @Override
    public FileTime creationTime() {
        return lastModifiedTime();
    }

    @Override
    public boolean isRegularFile() {
        return !isDirectory;
    }

    @Override
    public boolean isDirectory() {
        return isDirectory;
    }

    @Override
    public boolean isSymbolicLink() {
        return false;
    }

    @Override
    public boolean isOther() {
        return false;
    }

    @Override
    public long size() {
        if (objectResponse != null) {
            return objectResponse.contentLength();
        }
        return 0L; // Directories have size 0
    }

    @Override
    public Object fileKey() {
        return path;
    }
}