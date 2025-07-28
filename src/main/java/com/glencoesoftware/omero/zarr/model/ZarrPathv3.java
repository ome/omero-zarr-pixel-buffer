package com.glencoesoftware.omero.zarr.model;

import org.apache.maven.artifact.versioning.ComparableVersion;

import dev.zarr.zarrjava.store.StoreHandle;

public class ZarrPathv3 implements ZarrPath {

    private StoreHandle path;

    public ZarrPathv3(StoreHandle path) {
        this.path = path;
    }

    public StoreHandle getPath() {
        return path;
    }

    @Override
    public ZarrPath resolve(String path) {
        return new ZarrPathv3(this.path.resolve(path));
    }

    @Override
    public ComparableVersion getVersion() {
        return ZarrInfo.ZARR_V3;
    }
}
