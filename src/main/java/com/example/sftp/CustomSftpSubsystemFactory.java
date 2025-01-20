package com.example.sftp;

import org.apache.sshd.sftp.server.SftpSubsystemFactory;

public class CustomSftpSubsystemFactory extends SftpSubsystemFactory {
    public CustomSftpSubsystemFactory() {
        super();
        addSftpEventListener(new CustomSftpEventListener());
    }
}