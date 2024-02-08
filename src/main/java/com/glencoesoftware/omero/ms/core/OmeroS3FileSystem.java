package com.glencoesoftware.omero.ms.core;

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
