package com.glencoesoftware.omero.zarr.compat;

import java.io.IOException;
import java.util.Map;

/**
 * Interface for representing a path within a Zarr hierarchy. Provides methods for navigating the
 * Zarr structure, accessing metadata, and retrieving array information.
 */
public interface ZarrPath {

    /**
     * Resolves a relative path against this ZarrPath.
     *
     * @param path the relative path to resolve
     * @return a new ZarrPath representing the resolved path
     */
    public ZarrPath resolve(String path);

    /**
     * Retrieves the metadata associated with this Zarr path.
     *
     * @return a map containing the metadata key-value pairs
     * @throws IOException if an I/O error occurs while reading the metadata
     */
    public Map<String, Object> getMetadata() throws IOException;

    /**
     * Gets the Zarr array associated with this path.
     *
     * @return the ZArray object representing the array at this path
     * @throws IOException if an I/O error occurs while accessing the array
     */
    public ZArray getArray() throws IOException;
}
