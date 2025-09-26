package com.glencoesoftware.omero.zarr.compat;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.LoggerFactory;
import org.apache.maven.artifact.versioning.ComparableVersion;

import com.amazonaws.services.s3.AmazonS3;
import com.bc.zarr.ZarrGroup;
import com.google.common.base.Splitter;
import com.upplication.s3fs.OmeroS3FilesystemProvider;

import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.HttpStore;
import dev.zarr.zarrjava.store.S3Store;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Group;
import dev.zarr.zarrjava.v3.GroupMetadata;


/**
 * Tries to determine some properties of the zarr path,
 * if it's remote or local, and the zarr and ngff versions.
 * 
 * To access the zarr metadata/array use getZarrPath().
 */
public class ZarrInfo {
    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ZarrInfo.class);

    public static final ComparableVersion ZARR_V2 = new ComparableVersion("2");
    public static final ComparableVersion ZARR_V3 = new ComparableVersion("3");
    public static final ComparableVersion NGFF_V0_4 = new ComparableVersion("0.4");
    public static final ComparableVersion NGFF_V0_5 = new ComparableVersion("0.5");

    private ComparableVersion zarrVersion;

    private ComparableVersion ngffVersion;

    private String location;

    /**
     * Enum representing different storage types for Zarr data.
     */
    public enum StorageType {
        FILE,
        S3,
        HTTP
    }

    private StorageType storageType;

    public ZarrInfo(String location) throws IOException {
        this.location = location.endsWith("/") ? location.substring(0, location.length() - 1) : location;
        checkProperties();
    }

    /**
     * Tries to check some properties of the zarr path,
     * if it's remote or local, and the zarr and ngff versions.
     * @throws IOException 
     */
    private void checkProperties() throws IOException {
        URI uri;
        try {
            uri = new URI(location);
        } catch (URISyntaxException e) {
            throw new IOException("Invalid URI: " + location, e);
        }

        if (uri.getScheme() == null || "file".equals(uri.getScheme().toLowerCase())) {
            File test = new File(location);
            if (!test.isDirectory()) {
                throw new IOException("Not a directory: " + location);
            }
            if (!test.canRead()) {
                throw new IOException("Cannot read directory: " + location);
            }
            storageType = StorageType.FILE;
        } else {
            String scheme = uri.getScheme().toLowerCase();
            if (scheme.startsWith("http")) {
                storageType = StorageType.HTTP;
            } else if (scheme.equals("s3")) {
                storageType = StorageType.S3;
            } else {
                throw new IOException("Unsupported scheme: " + scheme);
            }
        }

        // checking for zarr v3
        try {
            StoreHandle sh = asStoreHandle();
            GroupMetadata md = Group.open(sh).metadata;
            zarrVersion = new ComparableVersion("3");  // if that works it should be v3
            try {
                ngffVersion = new ComparableVersion(((Map<String, Object>) md.attributes.get("ome")).get("version").toString());
            } catch (Exception e) {
                log.debug("Failed to get ngff version from zarr, set to 0.5");
                ngffVersion = new ComparableVersion("0.5");
            }
            return;
        } catch (Exception e) {
            log.debug("Not zarr v3:", e);
            // fall through
        }

        // checking for zarr v2
        try {
            Map<String, Object> attr = ZarrGroup.open(asPath(location)).getAttributes();
            zarrVersion = new ComparableVersion("2"); // if that works it must be v2
            try {
                List<Object> tmp = (List<Object>) attr.get("multiscales");
                ngffVersion = new ComparableVersion(((Map<String, Object>) tmp.get(0)).get("version").toString());
            } catch (Exception e) {
                log.debug("Failed to get ngff version from zarr, set to 0.4");
                ngffVersion = new ComparableVersion("0.4"); // if it's zarr v2 then we can actually safely assume it's ngff v0.4
            }
            return;
        } catch (Exception e) {
            log.debug("Not zarr v2:", e);
            throw new IOException("Failed to determine zarr version");
        }
    }

    /**
     * Gets the Zarr version.
     * @return the Zarr version as a string
     */
    public ComparableVersion getZarrVersion() {
        return zarrVersion;
    }

    /**
     * Gets the NGFF version.
     * @return the NGFF version as a string
     */
    public ComparableVersion getNgffVersion() {
        return ngffVersion;
    }

    /**
     * Gets the path.
     * @return the path
     */
    public String getLocation() {
        return location;
    }

    public ZarrPath getZarrPath() throws IOException {
        if (zarrVersion.equals(ZARR_V2)) {
            return new ZarrPathv2(asPath(location));
        } else {
            return new ZarrPathv3(asStoreHandle());
        }
    }

    /**
     * Converts an NGFF root string to a path, initializing a {@link FileSystem}
     * if required
     * For zarr version 2, resp NGFF 0.4
     * @return Fully initialized path or <code>null</code> if the NGFF root
     * directory has not been specified in configuration.
     * @throws IOException
     */
    private Path asPath(String location) throws IOException {
        try {
            URI uri = new URI(location);
            String scheme = uri.getScheme() != null ? uri.getScheme().toLowerCase() : "file";
            if (scheme.startsWith("http")) {
                String s3loc = location.replaceFirst("https?", "s3");
                return asPath(s3loc+"?anonymous=true");
            }
            if (scheme.equals("s3")) {
                if (uri.getUserInfo() != null && !uri.getUserInfo().isEmpty()) {
                    throw new RuntimeException(
                        "Found unsupported user information in S3 URI."
                        + " If you are trying to pass S3 credentials, "
                        + "use either named profiles or instance credentials.");
                }
                String query = Optional.ofNullable(uri.getQuery()).orElse("");
                Map<String, String> params = Splitter.on('&')
                        .trimResults()
                        .omitEmptyStrings()
                        .withKeyValueSeparator('=')
                        .split(query);
                // drop initial "/"
                String uriPath = uri.getPath().substring(1);
                int first = uriPath.indexOf("/");
                String bucket = "/" + uriPath.substring(0, first);
                String rest = uriPath.substring(first + 1);
                // FIXME: We might want to support additional S3FS settings in
                // the future.  See:
                //   * https://github.com/lasersonlab/Amazon-S3-FileSystem-NIO2
                Map<String, String> env = new HashMap<String, String>();
                String profile = params.get("profile");
                if (profile != null) {
                    env.put("s3fs_credential_profile_name", profile);
                }
                String anonymous =
                        Optional.ofNullable(params.get("anonymous"))
                                .orElse("false");
                env.put("s3fs_anonymous", anonymous);
                OmeroS3FilesystemProvider fsp = new OmeroS3FilesystemProvider();
                FileSystem fs = fsp.newFileSystem(uri, env);
                return fs.getPath(bucket, rest);
            }
        } catch (URISyntaxException e) {
            // we made sure earlier that location is a valid URI
        }
        return Path.of(location);
    }

    /**
     * Return a store handle.
     * For zarr version 3, resp NGFF > 0.5
     * @return
     */
    private StoreHandle asStoreHandle() {
        try {
            URI uri = new URI(location);
            String scheme = uri.getScheme() != null ? uri.getScheme().toLowerCase() : "file";
            String store = location.substring(0, location.lastIndexOf("/"));
            String zarr = location.substring(location.lastIndexOf("/")+1);
            zarr = zarr.replaceFirst("\\?.+", "");

            if (scheme.startsWith("http")) {
                return new HttpStore(store).resolve(zarr);
            }
            else if (scheme.startsWith("s3")) {
                String[] tmp = store.replaceFirst("s3://", "").split("/");
                String host = tmp[0];
                String bucket = tmp[1];
                String rest = String.join("/", Arrays.copyOfRange(tmp, 2, tmp.length));
                System.out.println("Host: " + host);

                // Use the OmeroS3FilesystemProvider to create AmazonS3 client
                String query = Optional.ofNullable(uri.getQuery()).orElse("");
                Map<String, String> params = Splitter.on('&')
                        .trimResults()
                        .omitEmptyStrings()
                        .withKeyValueSeparator('=')
                        .split(query);
                Map<String, String> env = new HashMap<String, String>();
                String profile = params.get("profile");
                if (profile != null) {
                    env.put("s3fs_credential_profile_name", profile);
                }
                String anonymous =
                        Optional.ofNullable(params.get("anonymous"))
                                .orElse("false");
                env.put("s3fs_anonymous", anonymous);
                OmeroS3FilesystemProvider fsp = new OmeroS3FilesystemProvider();
                AmazonS3 client = fsp.createAmazonS3(uri, env);

                // Create s3 client manually:
                // AmazonS3 client = AmazonS3ClientBuilder.standard()
                // .withEndpointConfiguration(new EndpointConfiguration(host, null))
                // .withCredentials(new AWSCredentialsProvider() {
                //     @Override
                //     public AWSCredentials getCredentials() {
                //         return new AnonymousAWSCredentials();
                //     }

                //     @Override
                //     public void refresh() {
                //         // do nothing
                //     }
                // })
                // .build();
                return new S3Store(client, bucket, rest).resolve(zarr);
            }
            else {
                return new FilesystemStore(store).resolve(zarr);
            }
        } catch (URISyntaxException e) {
            // we checked earlier that location is a valid URI
        }
        return null;
    }

    @Override
    public String toString() {
        return "ZarrInfo{" +
                "location='" + location + '\'' +
                ", storageType=" + storageType +
                ", zarrVersion=" + zarrVersion +
                ", ngffVersion=" + ngffVersion +
                '}';
    }

    public StorageType getStorageType() {
        return storageType;
    }
}
