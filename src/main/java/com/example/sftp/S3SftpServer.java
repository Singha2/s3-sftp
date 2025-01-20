package com.example.sftp;

import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.sftp.server.SftpSubsystemFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;

public class S3SftpServer {
    private final SshServer sshd;
    private final String bucketName;
   // private static volatile S3Client globalS3Client;
    private static final Object s3Lock = new Object();
    private final S3FileSystemFactory fsFactory;
    private final S3FileSystem fs;


    public S3SftpServer(int port, String bucketName) throws IOException {
        this.bucketName = bucketName;
        this.sshd = SshServer.setUpDefaultServer();
        this.sshd.setPort(port);
        this.sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider(Path.of("hostkey.ser")));

        CustomSftpSubsystemFactory sftpFactory = new CustomSftpSubsystemFactory();

        System.out.println("Initializing SFTP Server...");

        // Set specific username and password
        this.sshd.setPasswordAuthenticator((username, password, session) ->
                "admin".equals(username) && "password@123".equals(password));

        // Configure SFTP subsystem with S3 integration
        fsFactory = new S3FileSystemFactory(bucketName);

        this.sshd.setFileSystemFactory(fsFactory);
        //SftpSubsystemFactory sftpFactory = new SftpSubsystemFactory();
        sftpFactory.addSftpEventListener(new S3SftpEventListener());
        sftpFactory.setFileSystemAccessor(new S3SftpFileSystemAccessor(fsFactory.getFileSystem()));

        this.sshd.setSubsystemFactories(Collections.singletonList(sftpFactory));
        //fsFactory.addUserHomeDirectory("admin", "/home/admin");
        this.fs = fsFactory.getFileSystem();

        S3FileSystemProvider provider = (S3FileSystemProvider) fsFactory.getFileSystem().provider();
        provider.listS3Contents();

    }


    public void start() throws Exception {
        sshd.start();
        System.out.println("SFTP Server started on port " + sshd.getPort() + " backed by S3 bucket: " + bucketName);
    }

    public void stop() throws Exception {
        sshd.stop();
    }

    public void shutdown() {
        System.out.println("Shutting down S3SftpServer completely...");
        try {
            stop();
        } catch (Exception e) {
            System.err.println("Error stopping server: " + e.getMessage());
        } finally {
            this.fsFactory.shutdown();
        }
    }

    public static void main(String[] args) throws Exception {
        String bucketName = "customsftpfolderpath";
        S3SftpServer server = new S3SftpServer(2222, bucketName);
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown hook triggered");
            server.shutdown();
        }));

        server.start();

        // Keep the server running
        Thread.sleep(Long.MAX_VALUE);
    }
}
