package com.glencoesoftware.omero.ms.core;

import static com.upplication.s3fs.AmazonS3Factory.ACCESS_KEY;
import static com.upplication.s3fs.AmazonS3Factory.CONNECTION_TIMEOUT;
import static com.upplication.s3fs.AmazonS3Factory.MAX_CONNECTIONS;
import static com.upplication.s3fs.AmazonS3Factory.MAX_ERROR_RETRY;
import static com.upplication.s3fs.AmazonS3Factory.PATH_STYLE_ACCESS;
import static com.upplication.s3fs.AmazonS3Factory.PROTOCOL;
import static com.upplication.s3fs.AmazonS3Factory.PROXY_DOMAIN;
import static com.upplication.s3fs.AmazonS3Factory.PROXY_HOST;
import static com.upplication.s3fs.AmazonS3Factory.PROXY_PASSWORD;
import static com.upplication.s3fs.AmazonS3Factory.PROXY_PORT;
import static com.upplication.s3fs.AmazonS3Factory.PROXY_USERNAME;
import static com.upplication.s3fs.AmazonS3Factory.PROXY_WORKSTATION;
import static com.upplication.s3fs.AmazonS3Factory.REQUEST_METRIC_COLLECTOR_CLASS;
import static com.upplication.s3fs.AmazonS3Factory.SECRET_KEY;
import static com.upplication.s3fs.AmazonS3Factory.SIGNER_OVERRIDE;
import static com.upplication.s3fs.AmazonS3Factory.SOCKET_RECEIVE_BUFFER_SIZE_HINT;
import static com.upplication.s3fs.AmazonS3Factory.SOCKET_SEND_BUFFER_SIZE_HINT;
import static com.upplication.s3fs.AmazonS3Factory.SOCKET_TIMEOUT;
import static com.upplication.s3fs.AmazonS3Factory.USER_AGENT;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.internal.Constants;
import com.upplication.s3fs.S3FileSystem;
import com.upplication.s3fs.S3FileSystemProvider;

public class OmeroS3FileSystem extends S3FileSystem {

    private static final List<String> PROPS_TO_OVERLOAD = Arrays.asList(ACCESS_KEY, SECRET_KEY, REQUEST_METRIC_COLLECTOR_CLASS, CONNECTION_TIMEOUT, MAX_CONNECTIONS, MAX_ERROR_RETRY, PROTOCOL, PROXY_DOMAIN,
            PROXY_HOST, PROXY_PASSWORD, PROXY_PORT, PROXY_USERNAME, PROXY_WORKSTATION, SOCKET_SEND_BUFFER_SIZE_HINT, SOCKET_RECEIVE_BUFFER_SIZE_HINT, SOCKET_TIMEOUT,
            USER_AGENT, SIGNER_OVERRIDE, PATH_STYLE_ACCESS);

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
