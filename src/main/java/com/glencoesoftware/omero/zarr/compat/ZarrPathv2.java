package com.glencoesoftware.omero.zarr.compat;

import com.bc.zarr.ZarrArray;
import com.bc.zarr.ZarrGroup;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

class ZarrPathv2 implements ZarrPath {

    private Path path;

    public ZarrPathv2(Path path) {
        this.path = path;
    }

    @Override
    public ZarrPath resolve(String path) {
        return new ZarrPathv2(this.path.resolve(path));
    }

    @Override
    public Map<String, Object> getMetadata() throws IOException {
        return ZarrGroup.open(path).getAttributes();
    }

    @Override
    public ZArray getArray() throws IOException {
        return new ZArrayv2(ZarrArray.open(path));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ZarrPathv2 that = (ZarrPathv2) obj;
        return path.toString().equals(that.path.toString());
    }

    @Override
    public int hashCode() {
        return path.toString().hashCode();
    }

    @Override
    public String toString() {
        return path.toString();
    }
}
