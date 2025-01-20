package com.example.sftp;

import org.apache.sshd.common.session.Session;
import org.apache.sshd.server.session.ServerSession;
import org.apache.sshd.sftp.server.DirectoryHandle;
import org.apache.sshd.sftp.server.FileHandle;
import org.apache.sshd.sftp.server.SftpEventListener;

import java.io.IOException;
import java.nio.file.Path;

class CustomSftpEventListener implements SftpEventListener {
    @Override
    public void initialized(ServerSession session, int version) throws IOException {
        System.out.println("DEBUG: SFTP Session initialized with version: " + version);
    }

    @Override
    public void destroying(ServerSession session) {
        System.out.println("DEBUG: SFTP Session being destroyed");
    }

    @Override
    public void reading(ServerSession session, String remoteHandle, FileHandle localHandle,
                        long offset, byte[] data, int dataOffset, int dataLen) throws IOException {
        System.out.println("DEBUG: Reading directory: " + localHandle.getFile());
        // Put a breakpoint here
    }

    @Override
    public void writing(ServerSession session, String remoteHandle, FileHandle localHandle,
                        long offset, byte[] data, int dataOffset, int dataLen) throws IOException {
        System.out.println("DEBUG: Writing to file: " + localHandle.getFile());
    }
}
