package com.upplication.s3fs;

import static com.upplication.s3fs.AmazonS3Factory.ACCESS_KEY;
import static com.upplication.s3fs.AmazonS3Factory.SECRET_KEY;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.FileSystem;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.glencoesoftware.omero.ms.core.OmeroAmazonS3ClientFactory;
import com.glencoesoftware.omero.ms.core.OmeroS3FileSystem;
import com.glencoesoftware.omero.ms.core.OmeroS3ReadOnlySeekableByteChannel;
import com.google.common.base.Preconditions;

public class OmeroS3FilesystemProvider extends S3FileSystemProvider {

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

    private void validateProperties(Properties props) {
        Preconditions.checkArgument(
                (props.getProperty(ACCESS_KEY) == null && props.getProperty(SECRET_KEY) == null)
                        || (props.getProperty(ACCESS_KEY) != null && props.getProperty(SECRET_KEY) != null), "%s and %s should both be provided or should both be omitted",
                ACCESS_KEY, SECRET_KEY);
    }

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
     * Create the fileSystem
     *
     * @param uri   URI
     * @param props Properties
     * @return S3FileSystem never null
     */
    public S3FileSystem createFileSystem(URI uri, Properties props) {
        return new OmeroS3FileSystem(this, getFileSystemKey(uri, props), getAmazonS3(uri, props), uri.getHost());
    }

    protected AmazonS3Factory getAmazonS3Factory(Properties props) {
        return new OmeroAmazonS3ClientFactory();
    }

    @Override
    public void checkAccess(Path path, AccessMode... modes) throws IOException {
        // No-op
        return;
    }

    /**
     * check that the paths exists or not
     *
     * @param path S3Path
     * @return true if exists
     */
    @Override
    public boolean exists(S3Path path) {
        return true;
    }

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

    private S3Path toS3Path(Path path) {
        Preconditions.checkArgument(path instanceof S3Path, "path must be an instance of %s", S3Path.class.getName());
        return (S3Path) path;
    }

}
