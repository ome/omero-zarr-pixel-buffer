/*
 * Copyright (C) 2022 Glencoe Software, Inc. All rights reserved.
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

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.carlspring.cloud.storage.s3fs.S3ClientFactory;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.awscore.util.AwsHostNameUtils;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Subclass which maps an URI into a set of credentials to use for the client.
 */
public class OmeroAmazonS3ClientFactory extends S3ClientFactory {

    private static final org.slf4j.Logger log = LoggerFactory
        .getLogger(OmeroAmazonS3ClientFactory.class);

    private static final Map<String, S3Client> bucketClientMap = new HashMap<>();

    @Override
    protected AwsCredentialsProvider getCredentialsProvider(Properties props) {
        // If AWS Environment or System Properties are set, throw an exception
        // so users will know they are not supported
        if (System.getenv("AWS_ACCESS_KEY_ID") != null
            || System.getenv("AWS_SECRET_ACCESS_KEY") != null
            || System.getenv("AWS_SESSION_TOKEN") != null
            || System.getProperty("aws.accessKeyId") != null
            || System.getProperty("aws.secretAccessKey") != null) {
            throw new RuntimeException("AWS credentials supplied by environment variables"
                + " or Java system properties are not supported."
                + " Please use either named profiles or instance" + " profile credentials.");
        }
        boolean anonymous = Boolean.parseBoolean((String) props.get("s3fs_anonymous"));
        if (anonymous) {
            log.debug("Using anonymous credentials");
            return AnonymousCredentialsProvider.create();
        } else {
            String profileName = (String) props.get("s3fs_credential_profile_name");
            // Same instances and order from DefaultAWSCredentialsProviderChain
            ProfileCredentialsProvider.Builder profileBuilder = ProfileCredentialsProvider
                .builder();
            if (profileName != null) {
                profileBuilder.profileName(profileName);
            }
            return AwsCredentialsProviderChain.of(profileBuilder.build(),
                InstanceProfileCredentialsProvider.create());
        }
    }

    /**
     * Retrieves the bucket name from a given URI.
     *
     * @param uri The URI to handle
     * @return The bucket name
     */
    private String getBucketFromUri(URI uri) {
        String path = uri.getPath();
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        return path.substring(0, path.indexOf("/"));
    }

    /**
     * Retrieves the region from a given URI.
     *
     * @param uri The URI to handle
     * @return The region
     */
    private Region getRegionFromUri(URI uri) {
        Optional<Region> region = AwsHostNameUtils.parseSigningRegion(uri.getHost(), null);
        if (region.isPresent()) {
            return region.get();
        }
        return Region.US_EAST_1;
    }

    /**
     * Retrieves the endpoint from a given URI.
     *
     * @param uri The URI to handle
     * @return The endpoint
     */
    public String getEndPointFromUri(URI uri) {
        return "https://" + uri.getHost();
    }

    @Override
    public synchronized S3Client getS3Client(URI uri, Properties props) {
        // Check if we have a S3 client for this bucket
        String bucket = getBucketFromUri(uri);
        if (bucketClientMap.containsKey(bucket)) {
            log.info("Found bucket " + bucket);
            return bucketClientMap.get(bucket);
        }
        log.info("Creating client for bucket " + bucket);
        ClientOverrideConfiguration cconf = getOverrideConfiguration(props);
        S3Client client = S3Client.builder().credentialsProvider(getCredentialsProvider(props))
            .region(getRegionFromUri(uri)).endpointOverride(URI.create(getEndPointFromUri(uri)))
            .overrideConfiguration(cconf).build();
        bucketClientMap.put(bucket, client);
        return client;
    }

}
