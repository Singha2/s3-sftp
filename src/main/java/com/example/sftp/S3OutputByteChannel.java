package com.example.sftp;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

/**
 * Channel for writing files to S3. Used by SFTP for upload operations.
 * Buffers data in memory and uploads to S3 when the channel is closed.
 */
public class S3OutputByteChannel implements SeekableByteChannel {
    private final S3Client s3Client;
    private final String bucket;
    private final String key;
    private final ByteArrayOutputStream buffer;
    private boolean open;
    private long position;

    public S3OutputByteChannel(S3Client s3Client, String bucket, String key) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.key = key;
        this.buffer = new ByteArrayOutputStream();
        this.open = true;
        this.position = 0;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if (!open) {
            throw new IOException("Channel is closed");
        }

        // Copy data from the ByteBuffer to our internal buffer
        byte[] data = new byte[src.remaining()];
        src.get(data);
        buffer.write(data);

        // Update position and return number of bytes written
        position += data.length;
        return data.length;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() throws IOException {
        if (open) {
            try {
                // Upload the buffered content to S3
                byte[] data = buffer.toByteArray();
                PutObjectRequest request = PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .build();

                s3Client.putObject(request, RequestBody.fromBytes(data));
                buffer.close();
            } catch (S3Exception e) {
                throw new IOException("Failed to upload to S3: " + e.getMessage(), e);
            } finally {
                open = false;
            }
        }
    }

    @Override
    public int read(ByteBuffer dst) {
        throw new UnsupportedOperationException("Read not supported on write channel");
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public SeekableByteChannel position(long newPosition) {
        throw new UnsupportedOperationException("Position not supported on write channel");
    }

    @Override
    public long size() {
        return position;
    }

    @Override
    public SeekableByteChannel truncate(long size) {
        throw new UnsupportedOperationException("Truncate not supported");
    }
}
