package com.glencoesoftware.omero.zarr.model;

import java.nio.file.Path;

import org.apache.maven.artifact.versioning.ComparableVersion;

public class ZarrPathv2 implements ZarrPath {

    private Path path;

    public ZarrPathv2(Path path) {
        this.path = path;
    }

    public Path getPath() {
        return path;
    }

    @Override
    public ZarrPath resolve(String path) {
        return new ZarrPathv2(this.path.resolve(path));
    }

    @Override
    public ComparableVersion getVersion() {
        return ZarrInfo.ZARR_V2;
    }
}
