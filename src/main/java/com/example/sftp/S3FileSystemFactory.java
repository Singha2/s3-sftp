package com.example.sftp;

import org.apache.sshd.common.file.FileSystemFactory;
import org.apache.sshd.common.session.SessionContext;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class S3FileSystemFactory implements FileSystemFactory {
    private final String bucketName;
    private volatile S3Client s3Client;
    private volatile S3FileSystem fileSystem;
    private final Map<String, String> userHomeDirectories;
    private final Object lock = new Object();

    public S3FileSystemFactory(String bucketName) {
        System.out.println("Initializing S3FileSystemFactory for bucket: " + bucketName);
        this.bucketName = bucketName;
        this.userHomeDirectories = new ConcurrentHashMap<>();
        userHomeDirectories.put("admin", "/home/admin");
        initializeS3Client();
    }



    private void ensureValidS3Client() {
        if (s3Client == null) {
            initializeS3Client();
        } else {
            try {
                // Test the client with a simple operation
                s3Client.listBuckets();
            } catch (Exception e) {
                System.out.println("S3Client test failed, recreating: " + e.getMessage());
                synchronized (lock) {
                    try {
                        if (s3Client != null) {
                            s3Client.close();
                        }
                    } catch (Exception ce) {
                        System.out.println("Error closing old S3Client: " + ce.getMessage());
                    }
                    s3Client = null;
                    initializeS3Client();
                }
            }
        }
    }

    @Override
    public FileSystem createFileSystem(SessionContext session) throws IOException {
        System.out.println("Creating FileSystem for session: " + session);

        try {
            synchronized (lock) {
                ensureValidS3Client();

                if (fileSystem == null) {
                    fileSystem = new S3FileSystem(s3Client, bucketName);
                    System.out.println("Created new S3FileSystem");
                }

                // Don't initialize directories here - move it to a separate method
                return fileSystem;
            }
        } catch (Exception e) {
            System.err.println("Error in createFileSystem: " + e.getMessage());
            throw new IOException("Failed to create filesystem", e);
        }
    }

    public S3FileSystem getFileSystem() {
        synchronized (lock) {
            if (fileSystem == null) {
                ensureValidS3Client();
                fileSystem = new S3FileSystem(s3Client, bucketName);
            }
            return fileSystem;
        }
    }

    public void initializeDirectoryStructure() {
        System.out.println("Initializing directory structure");
        try {
            S3FileSystem fs = getFileSystem();
            createDirectoryIfNotExists(fs, "/home");
            createDirectoryIfNotExists(fs, "/home/admin");
        } catch (Exception e) {
            System.err.println("Error initializing directory structure: " + e.getMessage());
        }
    }

    private void createDirectoryIfNotExists(S3FileSystem fs, String pathStr) {
        try {
            Path path = fs.getPath(pathStr);
            if (!exists(fs, path)) {
                fs.provider().createDirectory(path);
                System.out.println("Created directory: " + pathStr);
            } else {
                System.out.println("Directory already exists: " + pathStr);
            }
        } catch (Exception e) {
            System.out.println("Note: Error handling directory " + pathStr + ": " + e.getMessage());
        }
    }

    private boolean exists(S3FileSystem fs, Path path) {
        try {
            fs.provider().checkAccess(path);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public Path getUserHomeDir(SessionContext session) throws IOException {
        String username = session.getUsername();
        String homePath = userHomeDirectories.getOrDefault(username, "/home/" + username);
        System.out.println("Getting home directory for user: " + username + " -> " + homePath);
        return getFileSystem().getPath(homePath);
    }

    // Call this method when shutting down the server
    public void shutdown() {
        System.out.println("S3FileSystemFactory shutdown called from:");
        Thread.dumpStack();

        synchronized (lock) {
            if (s3Client != null) {
                try {
                    System.out.println("Closing S3Client during factory shutdown");
                    s3Client.close();
                } catch (Exception e) {
                    System.err.println("Error closing S3Client: " + e.getMessage());
                } finally {
                    s3Client = null;
                    fileSystem = null;
                }
            } else {
                System.out.println("S3Client was already null during shutdown");
            }
        }
    }
}