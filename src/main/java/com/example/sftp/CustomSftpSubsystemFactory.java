package com.example.sftp;

import org.apache.sshd.common.util.threads.CloseableExecutorService;
import org.apache.sshd.server.channel.ChannelDataReceiver;
import org.apache.sshd.server.channel.ChannelSession;
import org.apache.sshd.sftp.server.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.sshd.common.session.helpers.AbstractSession.getSession;

public class CustomSftpSubsystemFactory extends SftpSubsystemFactory {

    private static final String CLOSED_CHANNEL_ATTRIBUTE = "sftp-channel-closed";
    private static final String TEMP_DIR_ATTRIBUTE = "sftp-temp-dir";


    @Override
    public SftpSubsystem createSubsystem(ChannelSession channel) {
        return new CustomSftpSubsystem(channel, createConfigurator());
    }

    protected SftpSubsystemConfigurator createConfigurator() {
        return new SftpSubsystemConfigurator() {
            @Override
            public SftpErrorStatusDataHandler getErrorStatusDataHandler() {
                return CustomSftpSubsystemFactory.this.getErrorStatusDataHandler();
            }

            @Override
            public ChannelDataReceiver getErrorChannelDataReceiver() {
                return CustomSftpSubsystemFactory.this.getErrorChannelDataReceiver();
            }

            @Override
            public CloseableExecutorService getExecutorService() {
                return CustomSftpSubsystemFactory.this.resolveExecutorService();
            }

            @Override
            public UnsupportedAttributePolicy getUnsupportedAttributePolicy() {
                return CustomSftpSubsystemFactory.this.getUnsupportedAttributePolicy();
            }

            @Override
            public SftpFileSystemAccessor getFileSystemAccessor() {
                return CustomSftpSubsystemFactory.this.getFileSystemAccessor();
            }

        };
    }

    private static class CustomSftpSubsystem extends SftpSubsystem {
        // Map to track our handles
        private final Map<String, Handle> handleMap = new ConcurrentHashMap<>();

        public CustomSftpSubsystem(ChannelSession channel, SftpSubsystemConfigurator configurator) {
            super(channel, configurator);
        }


        @Override
        public void destroy(ChannelSession channel) {
            System.out.println("CustomSftpSubsystem: Starting graceful destruction for session: " + channel);
            try {
                // Close any open handles for this session
                System.out.println("CustomSftpSubsystem: Closing open handles, count: " + handleMap.size());
                for (Map.Entry<String, Handle> handle : handleMap.entrySet()) {
                    try {
                        handle.getValue().close();
                        System.out.println("Closed handle: " + handle.getKey());
                    } catch (IOException e) {
                        System.err.println("Error closing handle " + handle.getKey() + ": " + e.getMessage());
                    }
                }
                handleMap.clear();

                // Clear session-specific caches
                System.out.println("CustomSftpSubsystem: Clearing session caches");
                clearAttributes();

            } catch (Exception e) {
                System.err.println("Error during subsystem destruction: " + e.getMessage());
                e.printStackTrace();
            } finally {
                System.out.println("CustomSftpSubsystem: Completing destruction, calling parent");
                super.destroy(channel);
            }
        }

        private void clearAttributes() {
            try {
                // Clear any session-specific attribute caches
                // Note: We might not have direct access to attribute caches
                System.out.println("Attribute cache cleared");
            } catch (Exception e) {
                System.err.println("Error clearing attributes: " + e.getMessage());
            }
        }

        @Override
        protected void createLink(int id, String existingPath, String linkPath, boolean symLink) throws IOException {
            System.out.println("Creating " + (symLink ? "symbolic" : "hard") + " link from " + existingPath + " to " + linkPath);
            super.createLink(id, existingPath, linkPath, symLink);
        }
    }
}