package com.example.sftp;



import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.sync.ResponseTransformer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;


class S3SeekableByteChannel implements SeekableByteChannel {
    private final S3Client s3Client;
    private final String bucket;
    private final String key;
    private long position;
    private boolean open;
    private final long size;

    public S3SeekableByteChannel(S3Client s3Client, String bucket, String key) throws IOException {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.key = key;
        this.position = 0;
        this.open = true;

        try {
            HeadObjectResponse response = s3Client.headObject(HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build());
            this.size = response.contentLength();
        } catch (S3Exception e) {
            throw new IOException("Failed to get object size", e);
        }
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (!open) {
            throw new IOException("Channel is closed");
        }

        try {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .range("bytes=" + position + "-" + (position + dst.remaining() - 1))
                    .build();

            byte[] data = s3Client.getObject(request, ResponseTransformer.toBytes()).asByteArray();
            if (data.length == 0) {
                return -1;
            }

            dst.put(data);
            position += data.length;
            return data.length;
        } catch (S3Exception e) {
            throw new IOException("Failed to read from S3", e);
        }
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        throw new UnsupportedOperationException("Write not supported on read channel");
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public SeekableByteChannel position(long newPosition) {
        this.position = newPosition;
        return this;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public SeekableByteChannel truncate(long size) {
        throw new UnsupportedOperationException("Truncate not supported");
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() {
        open = false;
    }
}
