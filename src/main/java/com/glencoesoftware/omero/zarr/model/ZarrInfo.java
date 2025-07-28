package com.glencoesoftware.omero.zarr.model;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.LoggerFactory;

import org.apache.maven.artifact.versioning.ComparableVersion;

import com.amazonaws.services.s3.AmazonS3ClientBuilder;
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
 * To access the zarr data use:
 * For zarr version 2, resp NGFF 0.4: asPath()
 * For zarr version 3, resp NGFF > 0.5: asStoreHandle()
 */
public class ZarrInfo {
    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(ZarrInfo.class);

    public static final ComparableVersion ZARR_V2 = new ComparableVersion("2");
    public static final ComparableVersion ZARR_V3 = new ComparableVersion("3");
    public static final ComparableVersion NGFF_V0_4 = new ComparableVersion("0.4");

    private ComparableVersion zarrVersion;

    private ComparableVersion ngffVersion;

    private String location;

    private boolean remote;

    public ZarrInfo(String location) {
        this.location = location.endsWith("/") ? location.substring(0, location.length() - 1) : location;
        checkProperties();
        log.info("Initialized ZarrPath: " + location);
        log.info("Remote store: " + remote);
        log.info("Zarr version: " + zarrVersion);
        log.info("NGFF version: " + ngffVersion);
    }

    /**
     * Tries to check some properties of the zarr path,
     * if it's remote or local, and the zarr and ngff versions.
     */
    private void checkProperties() {
        this.remote = location.toLowerCase().startsWith("http://") ||
                location.toLowerCase().startsWith("https://") ||
                location.toLowerCase().startsWith("s3://");

        // checking for zarr v2
        try {
            Map<String, Object> attr = ZarrGroup.open(asPath(location)).getAttributes();
            zarrVersion = new ComparableVersion("2"); // if that works it must be v2
            List<Object> tmp = (List<Object>) attr.get("multiscales");
            ngffVersion = new ComparableVersion(((Map<String, Object>) tmp.get(0)).get("version").toString());
            return;
        } catch (Exception e) {
            // fall through
        }

        // checking for zarr v3
        try {
            StoreHandle sh = asStoreHandle();
            GroupMetadata md = Group.open(sh).metadata;
            if (md.attributes.containsKey("zarr_format")) {
                zarrVersion = new ComparableVersion(md.attributes.get("zarr_format").toString());
            } else {
                zarrVersion = new ComparableVersion("3");
            }
            ngffVersion = new ComparableVersion(((Map<String, Object>) md.attributes.get("ome")).get("version").toString());
        } catch (Exception e) {
            // fall through
        }

        // set reasonable defaults
        if (zarrVersion == null) {
            if (ngffVersion != null && ngffVersion.compareTo(NGFF_V0_4) > 0) {
                zarrVersion = new ComparableVersion("3");
            } else {
                zarrVersion = new ComparableVersion("2");
            }
            log.warn("No zarr version found, default to " + zarrVersion);
        }
        if (ngffVersion == null) {
            if (zarrVersion != null && zarrVersion.compareTo(ZARR_V2) > 0) {
                ngffVersion = new ComparableVersion("0.5");
            } else {
                ngffVersion = new ComparableVersion("0.4");
            }
            log.warn("No NGFF version found, default to " + ngffVersion);
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
            return new ZarrPathv2(asPath());
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
    public Path asPath() throws IOException {
        if (location.isEmpty()) {
            return null;
        }
        return asPath(location);
    }

    public static Path asPath(String location) throws IOException {
        try {
            URI uri = new URI(location);
            if ("s3".equals(uri.getScheme())) {
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
            // Fall through
        }
        return Paths.get(location);
    }

    /**
     * Checks if this is a remote store.
     * @return true if remote, false if local
     */
    public boolean isRemote() {
        return remote;
    }

    public StoreHandle asStoreHandle() {
        return asStoreHandle(location);
    }

    /**
     * Return a store handle.
     * For zarr version 3, resp NGFF > 0.5
     * @return
     */
    public static StoreHandle asStoreHandle(String location) {
        //TODO: Properly parse the URI to get store/endpoint, bucket, etc.

        if (location.toLowerCase().startsWith("http://") || location.toLowerCase().startsWith("https://")) {
            String store = location.substring(0, location.lastIndexOf("/"));
            String zarr = location.substring(location.lastIndexOf("/")+1);
            return new HttpStore(store).resolve(zarr);
        }
        else if (location.toLowerCase().startsWith("s3://")) {
            // TODO: This def won't work like that (see comment above)
            String store = location.substring(0, location.lastIndexOf("/"));
            String zarr = location.substring(location.lastIndexOf("/")+1);
            return new S3Store(AmazonS3ClientBuilder.standard().build(), store, zarr).resolve("");
        }
        String store = location.substring(0, location.lastIndexOf("/"));
        String zarr = location.substring(location.lastIndexOf("/")+1);
        return new FilesystemStore(store).resolve(zarr);
    }
}
