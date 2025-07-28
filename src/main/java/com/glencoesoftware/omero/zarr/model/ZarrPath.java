package com.glencoesoftware.omero.zarr.model;

import org.apache.maven.artifact.versioning.ComparableVersion;

public interface ZarrPath {

    public Object getPath();

    public ZarrPath resolve(String path);

    public ComparableVersion getVersion();

}
