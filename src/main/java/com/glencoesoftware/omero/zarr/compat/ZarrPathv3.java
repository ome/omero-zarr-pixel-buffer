package com.glencoesoftware.omero.zarr.compat;

import java.io.IOException;
import java.util.Map;

import org.apache.maven.artifact.versioning.ComparableVersion;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.Group;

class ZarrPathv3 implements ZarrPath {

    private StoreHandle path;
    private Group group;
    private String key = null;

    public ZarrPathv3(StoreHandle path) {
        try {
            this.path = path;
            this.group = Group.open(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private ZarrPathv3(ZarrPathv3 parent, String resolvePath) {
        // this can either resolve to another group or an array
        this.path = parent.path.resolve(resolvePath);
        try {
            // it's a group
            this.group = Group.open(this.path);
        } catch (IOException e) {
            // is not a group; points to array from parent group
            // with resolvePath as key.
            this.group = parent.group;
            this.key = resolvePath;
        }
    }

    @Override
    public ZarrPath resolve(String resolvePath) {
        return new ZarrPathv3(this, resolvePath);
    }

    @Override
    public ComparableVersion getVersion() {
        return ZarrInfo.ZARR_V3;
    }

    @Override
    public Map<String, Object> getMetadata() throws IOException {
        return group.metadata.attributes;
    }

    @Override
    public ZArray getArray() throws IOException {
        Array array;
        try {
            array = (Array) group.get(key);
        } catch (ZarrException e) {
            throw new IOException(e);
        }
        return new ZArrayv3(array);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ZarrPathv3 that = (ZarrPathv3) obj;
        return path.toString().equals(that.path.toString());
    }

    @Override
    public int hashCode() {
        return path.toString().hashCode();
    }

    @Override
    public String toString() {
        return key == null ? "(Group) "+ path.toString() : "(Array) " + path.toString();
    }
}
