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

import java.io.IOException;

import com.amazonaws.services.s3.AmazonS3;
import com.upplication.s3fs.S3FileSystem;
import com.upplication.s3fs.S3FileSystemProvider;

public class OmeroS3FileSystem extends S3FileSystem {

    public OmeroS3FileSystem(S3FileSystemProvider provider, String key,
            AmazonS3 client, String endpoint) {
        super(provider, key, client, endpoint);
    }

    @Override
    public void close() throws IOException {
        //No-op
    }

    @Override
    public boolean isOpen() {
        return true; //Not possible to be closed
    }
}
