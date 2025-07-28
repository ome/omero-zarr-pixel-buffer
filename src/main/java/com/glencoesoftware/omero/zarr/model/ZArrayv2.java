package com.glencoesoftware.omero.zarr.model;

import java.io.IOException;

import com.bc.zarr.DataType;
import com.bc.zarr.ZarrArray;

import loci.formats.FormatTools;
import ucar.ma2.InvalidRangeException;

public class ZArrayv2 implements ZArray {
    
    private ZarrArray array;

    public ZArrayv2(ZarrArray array) {
        this.array = array;
    }

    @Override
    public int[] getShape() {
        return array.getShape();
    }

    @Override
    public int[] getChunks() {
        return array.getChunks();
    }

    @Override
    public void read(byte[] buffer, int[] shape, int[] offset) throws IOException, InvalidRangeException {
        array.read(buffer, shape, offset);
    }

    @Override
    public Object read(int[] shape, int[] offset) throws IOException, InvalidRangeException {
        return array.read(shape, offset);
    }

    @Override
    /**
     * Get Bio-Formats/OMERO pixels type for buffer.
     * @return See above.
     */
    public int getPixelsType() {
        DataType dataType = array.getDataType();
        switch (dataType) {
            case u1:
                return FormatTools.UINT8;
            case i1:
                return FormatTools.INT8;
            case u2:
                return FormatTools.UINT16;
            case i2:
                return FormatTools.INT16;
            case u4:
                return FormatTools.UINT32;
            case i4:
                return FormatTools.INT32;
            case f4:
                return FormatTools.FLOAT;
            case f8:
                return FormatTools.DOUBLE;
            default:
                throw new IllegalArgumentException(
                        "Data type " + dataType + " not supported");
        }
    }
}
