/*
 * Copyright (C) 2025 Glencoe Software, Inc. All rights reserved.
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

package com.bc.zarr.storage;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.Path;

/**
 * Overridden FileSystemStore. Implemented to catch exceptions
 * when missing chunks are encountered rather than allowing them to propogate
 * causing errors.
 */
public class OmeroFileSystemStore extends FileSystemStore {
    
    /**
     * Constructor.
     *
     * @param path The path to the zarr root
     * @param fileSystem The FileSystem used to access the file
     */
    public OmeroFileSystemStore(String path, FileSystem fileSystem) {
        super(path, fileSystem);
    }

    /**
     * Constructor.
     *
     * @param rootPath Path to the zarr root
     */
    public OmeroFileSystemStore(Path rootPath) {
        super(rootPath);
    }
    
    /**
     * Gets an input stream for the file contents if it exists and is readable,
     * otherwise returns <code>null</code>. Because <code>checkAccess</code> is
     * implemented as a No-Op in {@link OmeroS3FilesystemProvider}, we will
     * always attempt to read files in S3. This results in an AmazonS3Exception
     * with a status code of 404 for missing chunks. Rather than allow this
     * exception to propagate, we catch it here and treat it as if the file
     * was not readable.
     *
     * @param key The key (relative to the root) of the file to read
     * @return {@link InputStream} for the file contents if successful,
     *     <code>null</code> otherwise
     */
    @Override
    public InputStream getInputStream(String key) throws IOException {
        try {
            return super.getInputStream(key);
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() != 404) {
                throw e;
            }
            return null;
        }
    }

}
