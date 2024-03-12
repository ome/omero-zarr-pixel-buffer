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
package com.upplication.s3fs;

import static com.upplication.s3fs.AmazonS3Factory.ACCESS_KEY;
import static com.upplication.s3fs.AmazonS3Factory.SECRET_KEY;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.glencoesoftware.omero.ms.core.OmeroAmazonS3ClientFactory;
import com.glencoesoftware.omero.ms.core.OmeroS3FileSystem;
import com.glencoesoftware.omero.ms.core.OmeroS3ReadOnlySeekableByteChannel;
import com.google.common.base.Preconditions;
import com.upplication.s3fs.attribute.S3BasicFileAttributes;

public class OmeroS3FilesystemProvider extends S3FileSystemProvider {

    /**
     * Overridden, hybrid version of the implementation from
     * {@link S3FileSystemProvider#newFileSystem(URI, Map)}.  Our implementation
     * ensures that a new filesystem is created every time and not registered
     * with the JVM wide <code>fileSystems</code> map. 
     */
    @Override
    public FileSystem newFileSystem(URI uri, Map<String, ?> env) {
        validateUri(uri);
        // get properties for the env or properties or system
        Properties props = getProperties(uri, env);
        validateProperties(props);
        // create the filesystem with the final properties, store and return
        S3FileSystem fileSystem = createFileSystem(uri, props);
        return fileSystem;
    }

    /**
     * An exact copy of the implementation from {@link S3FileSystemProvider}
     * due to its private visibility.
     */
    private void validateProperties(Properties props) {
        Preconditions.checkArgument(
                (props.getProperty(ACCESS_KEY) == null && props.getProperty(SECRET_KEY) == null)
                        || (props.getProperty(ACCESS_KEY) != null && props.getProperty(SECRET_KEY) != null), "%s and %s should both be provided or should both be omitted",
                ACCESS_KEY, SECRET_KEY);
    }

    /**
     * An exact copy of the implementation from {@link S3FileSystemProvider}
     * due to its private visibility.
     */
    private Properties getProperties(URI uri, Map<String, ?> env) {
        Properties props = loadAmazonProperties();
        addEnvProperties(props, env);
        // and access key and secret key can be override
        String userInfo = uri.getUserInfo();
        if (userInfo != null) {
            String[] keys = userInfo.split(":");
            props.setProperty(ACCESS_KEY, keys[0]);
            if (keys.length > 1) {
                props.setProperty(SECRET_KEY, keys[1]);
            }
        }
        return props;
    }


    /**
     * Overridden, hybrid version of the implementation from
     * {@link S3FileSystemProvider#createFileSystem(URI, Properties)}.  Our
     * implementation uses our own {@link OmeroS3FileSystem}.
     */
    @Override
    public S3FileSystem createFileSystem(URI uri, Properties props) {
        return new OmeroS3FileSystem(this, getFileSystemKey(uri, props), getAmazonS3(uri, props), uri.getHost());
    }

    /**
     * Overridden, hybrid version of the implementation from
     * {@link S3FileSystemProvider#getAmazonS3Factory(Properties)}.  Our
     * implementation uses our own {@link OmeroAmazonS3ClientFactory}.
     */
    @Override
    protected AmazonS3Factory getAmazonS3Factory(Properties props) {
        return new OmeroAmazonS3ClientFactory();
    }

    /**
     * Overridden, hybrid version of the implementation from
     * {@link S3FileSystemProvider#checkAccess(Path, AccessMode...)}.  Our
     * implementation is a no-op, effectively disabling access checks.
     */
    @Override
    public void checkAccess(Path path, AccessMode... modes) throws IOException {
        // No-op
        return;
    }

    /**
     * Overridden, hybrid version of the implementation from
     * {@link S3FileSystemProvider#exists(S3Path)}.  Our
     * implementation is a no-op, effectively disabling existence checks.
     */
    @Override
    public boolean exists(S3Path path) {
        return true;
    }

    /**
     * Overridden, hybrid version of the implementation from
     * {@link S3FileSystemProvider#newByteChannel(Path, Set, FileAttribute...)}.
     * Our implementation uses our own
     * {@link OmeroS3ReadOnlySeekableByteChannel}.
     */
    @Override
    public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        S3Path s3Path = toS3Path(path);
        if (options.isEmpty() || options.contains(StandardOpenOption.READ)) {
            if (options.contains(StandardOpenOption.WRITE))
                throw new UnsupportedOperationException(
                    "Can't read and write one on channel"
                );
            return new OmeroS3ReadOnlySeekableByteChannel(s3Path, options);
        } else {
            return new S3SeekableByteChannel(s3Path, options);
        }
    }

    /**
     * An exact copy of the implementation from {@link S3FileSystemProvider}
     * due to its private visibility.
     */
    private S3Path toS3Path(Path path) {
        Preconditions.checkArgument(path instanceof S3Path, "path must be an instance of %s", S3Path.class.getName());
        return (S3Path) path;
    }

    /**
     * Overridden, hybrid version of the implementation from
     * {@link S3FileSystemProvider#readAttributes(Path, Class, LinkOption...)}.
     * Our implementation is a no-op, effectively using the same set of
     * read-only attributes for every file.
     */
    @Override
    public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        S3Path s3path = (S3Path) path;
        BasicFileAttributes attrs = new S3BasicFileAttributes(s3path.getKey(), null, 0, true, false);
        return type.cast(attrs);
    }
}
