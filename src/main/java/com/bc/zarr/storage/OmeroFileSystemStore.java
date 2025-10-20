package com.bc.zarr.storage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.amazonaws.services.s3.model.AmazonS3Exception;

public class OmeroFileSystemStore extends FileSystemStore {

    private Path omeroInternalRoot; //Field in FileSystemStore is private
    
    public OmeroFileSystemStore(String path, FileSystem fileSystem) {
        super(path, fileSystem);
        if (fileSystem == null) {
            omeroInternalRoot = Paths.get(path);
        } else {
            omeroInternalRoot = fileSystem.getPath(path);
        }
    }

    public OmeroFileSystemStore(Path rootPath) {
        super(rootPath);
        omeroInternalRoot = rootPath;
    }
    
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
