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
