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
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Overridden FileSystemStore. Implemented to catch exceptions
 * when missing chunks are encountered rather than allowing them to propogate
 * causing errors.
 */
public class OmeroFileSystemStore extends FileSystemStore {

    private Path omeroInternalRoot; // Field in FileSystemStore is private
    
    /**
     * Constructor.
     *
     * @param path The path to the zarr root
     * @param fileSystem The FileSystem used to access the file
     */
    public OmeroFileSystemStore(String path, FileSystem fileSystem) {
        super(path, fileSystem);
        if (fileSystem == null) {
            omeroInternalRoot = Paths.get(path);
        } else {
            omeroInternalRoot = fileSystem.getPath(path);
        }
    }

    /**
     * Constructor.
     *
     * @param rootPath Path to the zarr root
     */
    public OmeroFileSystemStore(Path rootPath) {
        super(rootPath);
        omeroInternalRoot = rootPath;
    }
    
    /**
     * Gets an input stream for the file contents if it exists and is readable,
     * otherwise returns <code>null</code>.
     *
     * @param key The key (relative to the root) of the file to read
     * @return {@link InputStream} for the file contents if successful,
     *     <code>null</code> otherwise
     */
    @Override
    public InputStream getInputStream(String key) throws IOException {
        final Path path = omeroInternalRoot.resolve(key);
        if (Files.isReadable(path)) {
            try {
                byte[] bytes = Files.readAllBytes(path);
                return new ByteArrayInputStream(bytes);
            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() != 404) {
                    throw e;
                }
                return null;
            }
        }
        return null;
    }

}
