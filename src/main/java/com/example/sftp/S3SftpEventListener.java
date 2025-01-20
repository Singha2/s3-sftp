package com.example.sftp;


import org.apache.sshd.server.session.ServerSession;
import org.apache.sshd.sftp.server.SftpEventListener;

import org.apache.sshd.sftp.server.FileHandle;


public class S3SftpEventListener implements SftpEventListener {
    @Override
    public void initialized(ServerSession session, int version) {
        System.out.println("Client connected with SFTP version: " + version);
    }

    @Override
    public void destroying(ServerSession session) {
        System.out.println("Client disconnected");
    }

    @Override
    public void written(ServerSession session, String remoteHandle, FileHandle localHandle, long offset, byte[] data, int dataOffset, int dataLen, Throwable thrown) {
        if (thrown == null) {
            System.out.println("File write operation - offset: " + offset + ", length: " + dataLen);
        } else {
            System.err.println("Write error: " + thrown.getMessage());
        }
    }

    @Override
    public void read(ServerSession session, String remoteHandle, FileHandle localHandle, long offset, byte[] data, int dataOffset, int dataLen, int readLen, Throwable thrown) {
        if (thrown == null) {
            System.out.println("File read operation - offset: " + offset + ", length: " + dataLen);
        } else {
            System.err.println("Read error: " + thrown.getMessage());
        }
    }
}
