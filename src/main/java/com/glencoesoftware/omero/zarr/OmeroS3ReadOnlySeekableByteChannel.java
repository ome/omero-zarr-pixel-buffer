/*
 * Copyright (C) 2024 Glencoe Software, Inc. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
package com.glencoesoftware.omero.zarr;


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

import org.perf4j.slf4j.Slf4JStopWatch;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.upplication.s3fs.S3Path;
import com.upplication.s3fs.S3ReadOnlySeekableByteChannel;

/**
 * Overridden, hybrid version of the implementation from
 * {@link S3ReadOnlySeekableByteChannel}.  Due to its private visibility on
 * nearly all the important instance variables much of the implementation is
 * copied verbatim.
 */
public class OmeroS3ReadOnlySeekableByteChannel implements SeekableByteChannel {

    private Set<? extends OpenOption> options;
    private final long length;
    byte[] data;
    private final ReadableByteChannel rbc;
    private long position = 0;

    /**
     * Overridden, hybrid version of the implementation from
     * {@link S3ReadOnlySeekableByteChannel}.  Our implementation loads the
     * entire object in full from S3 without checks for length during
     * object construction.
     */
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

        String bucketName = path.getFileStore().name();
        String key = path.getKey();
        GetObjectRequest getObjectRequest =
            new GetObjectRequest(bucketName, key);

        Slf4JStopWatch t0 = new Slf4JStopWatch(
                "OmeroS3ReadOnlySeekableByteChannel.getObject",
                "s3://" + bucketName + "/" + key);
        try {
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
        } finally {
            t0.stop();
        }
        this.length = this.data.length;
        ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
        rbc = Channels.newChannel(inputStream);
        this.position = 0;
    }

    /**
     * An exact copy of the implementation from
     * {@link S3ReadOnlySeekableByteChannel} due to its instance variable
     * private visibility.
     */
    @Override
    public boolean isOpen() {
        return rbc.isOpen();
    }

    /**
     * An exact copy of the implementation from
     * {@link S3ReadOnlySeekableByteChannel} due to its instance variable
     * private visibility.
     */
    @Override
    public long position() { return position; }

    /**
     * Overridden, hybrid version of the implementation from
     * {@link S3ReadOnlySeekableByteChannel}.  Our implementation does not
     * support repositioning within the channel.
     */
    @Override
    public SeekableByteChannel position(long targetPosition)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * An exact copy of the implementation from
     * {@link S3ReadOnlySeekableByteChannel} due to its instance variable
     * private visibility.
     */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        int n = rbc.read(dst);
        if (n > 0) {
            position += n;
        }
        return n;
    }

    /**
     * An exact copy of the implementation from
     * {@link S3ReadOnlySeekableByteChannel} due to its instance variable
     * private visibility.
     */
    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        throw new NonWritableChannelException();
    }

    /**
     * An exact copy of the implementation from
     * {@link S3ReadOnlySeekableByteChannel} due to its instance variable
     * private visibility.
     */
    @Override
    public int write(ByteBuffer src) throws IOException {
        throw new NonWritableChannelException();
    }

    /**
     * An exact copy of the implementation from
     * {@link S3ReadOnlySeekableByteChannel} due to its instance variable
     * private visibility.
     */
    @Override
    public long size() throws IOException {
        return length;
    }

    /**
     * An exact copy of the implementation from
     * {@link S3ReadOnlySeekableByteChannel} due to its instance variable
     * private visibility.
     */
    @Override
    public void close() throws IOException {
        rbc.close();
    }
}
