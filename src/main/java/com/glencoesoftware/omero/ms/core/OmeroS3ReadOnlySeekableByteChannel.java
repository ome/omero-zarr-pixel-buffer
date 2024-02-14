package com.glencoesoftware.omero.ms.core;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.OpenOption;
import java.nio.file.ReadOnlyFileSystemException;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.upplication.s3fs.S3Path;

public class OmeroS3ReadOnlySeekableByteChannel implements SeekableByteChannel {

    private Set<? extends OpenOption> options;
    private long length;
    byte[] data;
    private ReadableByteChannel rbc;
    private long position = 0;

    public OmeroS3ReadOnlySeekableByteChannel(S3Path path, Set<? extends OpenOption> options) throws IOException {
        this.options = Collections.unmodifiableSet(new HashSet<>(options));

        if (
            this.options.contains(StandardOpenOption.WRITE) ||
            this.options.contains(StandardOpenOption.CREATE) ||
            this.options.contains(StandardOpenOption.CREATE_NEW) ||
            this.options.contains(StandardOpenOption.APPEND)
        ) {
            throw new ReadOnlyFileSystemException();
        }

        if (rbc != null) {
            rbc.close();
        }

        GetObjectRequest getObjectRequest =
            new GetObjectRequest(
                path.getFileStore().name(),
                path.getKey()
            );

        S3Object s3Object =
            path
                .getFileSystem()
                .getClient()
                .getObject(getObjectRequest);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        // The return value of getObjectContent should be copied and
        // the stream closed as quickly as possible. See
        // https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/S3Object.html#getObjectContent--
        try (S3ObjectInputStream s3Stream = s3Object.getObjectContent()) {
            byte[] read_buf = new byte[1024*1024];
            int read_len = 0;
            while ((read_len = s3Stream.read(read_buf)) > 0) {
                outputStream.write(read_buf, 0, read_len);
            }
        }
        this.data = outputStream.toByteArray();
        this.length = this.data.length;
        ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
        rbc = Channels.newChannel(inputStream);
        this.position = 0;
    }

    @Override
    public void close() throws IOException {
        rbc.close();
    }

    @Override
    public boolean isOpen() {
        return rbc.isOpen();
    }

    @Override
    public long position() throws IOException {
        return position;
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
        rbc = Channels.newChannel(inputStream);
        inputStream.skip(newPosition);
        position = newPosition;
        return this;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int n = rbc.read(dst);
        if (n > 0) {
            position += n;
        }
        return n;
    }

    @Override
    public long size() throws IOException {
        return this.length;
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        throw new NonWritableChannelException();
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        throw new NonWritableChannelException();
    }

}
