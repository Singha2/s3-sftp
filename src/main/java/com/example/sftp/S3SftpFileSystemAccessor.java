package com.example.sftp;

import org.apache.sshd.common.util.ValidateUtils;
import org.apache.sshd.server.session.ServerSession;
import org.apache.sshd.sftp.server.SftpFileSystemAccessor;
import org.apache.sshd.sftp.server.SftpSubsystemProxy;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

public class S3SftpFileSystemAccessor implements SftpFileSystemAccessor {
    private final S3FileSystem fileSystem;
    private final AtomicReference<Path> currentDir;

    public S3SftpFileSystemAccessor(S3FileSystem fileSystem) {
        this.fileSystem = ValidateUtils.checkNotNull(fileSystem, "No filesystem provided");
        this.currentDir = new AtomicReference<>(fileSystem.getPath("/home/admin"));

        // Ensure base directories exist
        try {
            fileSystem.provider().createDirectory(fileSystem.getPath("/home"));
            fileSystem.provider().createDirectory(fileSystem.getPath("/home/admin"));
        } catch (IOException e) {
            // Ignore if directories already exist
        }
    }

    @Override
    public Path resolveLocalFilePath(SftpSubsystemProxy subsystem, Path rootDir, String remotePath)
            throws IOException, InvalidPathException {

        System.out.println("\n=== Resolving Path ===");
        System.out.println("Remote path: " + remotePath);
        System.out.println("Current directory: " + currentDir.get());

        if (remotePath == null || remotePath.isEmpty()) {
            System.out.println("Empty path, returning current directory");
            return currentDir.get();
        }

        // Handle special cases
        if (remotePath.equals(".")) {
            System.out.println("Current directory requested");
            return currentDir.get();
        }
        if (remotePath.equals("..")) {
            Path parent = currentDir.get().getParent();
            Path result = parent != null ? parent : fileSystem.getPath("/");
            System.out.println("Parent directory requested: " + result);
            return result;
        }

        // Convert path to absolute if it's relative
        Path resolvedPath;
        if (remotePath.startsWith("/")) {
            resolvedPath = fileSystem.getPath(remotePath);
            System.out.println("Absolute path resolved: " + resolvedPath);
        } else {
            resolvedPath = currentDir.get().resolve(remotePath);
            System.out.println("Relative path resolved: " + resolvedPath);
        }

        // Normalize the path
        resolvedPath = resolvedPath.normalize();
        System.out.println("Normalized path: " + resolvedPath);

        return resolvedPath;
    }

    @Override
    public String toString() {
        return "S3SftpFileSystemAccessor[currentDir=" + currentDir.get() + "]";
    }
}