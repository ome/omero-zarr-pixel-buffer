package com.glencoesoftware.omero.zarr.compat;

import java.io.IOException;
import java.util.Map;

import org.apache.maven.artifact.versioning.ComparableVersion;

public interface ZarrPath {

    public ZarrPath resolve(String path);

    public ComparableVersion getVersion();

    public Map<String, Object> getMetadata() throws IOException;

    public ZArray getArray() throws IOException;
}
