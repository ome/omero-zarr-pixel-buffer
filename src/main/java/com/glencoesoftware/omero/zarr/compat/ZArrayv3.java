package com.glencoesoftware.omero.zarr.compat;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.DataType;
import java.io.IOException;
import java.nio.ByteBuffer;
import loci.formats.FormatTools;
import ucar.ma2.InvalidRangeException;

class ZArrayv3 implements ZArray {

    private Array array;

    public ZArrayv3(Array array) {
        this.array = array;
    }

    @Override
    public int[] getShape() {
        int[] shape = new int[this.array.metadata().shape.length];
        for (int i = 0; i < this.array.metadata().shape.length; i++) {
            shape[i] = (int) this.array.metadata().shape[i];
        }
        return shape;
    }

    @Override
    public int[] getChunks() {
        int[] chunks = new int[this.array.metadata().chunkShape().length];
        for (int i = 0; i < this.array.metadata().chunkShape().length; i++) {
            chunks[i] = (int) this.array.metadata().chunkShape()[i];
        }
        return chunks;
    }

    @Override
    public void read(byte[] buffer, int[] shape, int[] offset)
        throws IOException, InvalidRangeException {
        try {
            long[] offsetLong = new long[offset.length];
            for (int i = 0; i < offset.length; i++) {
                offsetLong[i] = offset[i];
            }
            ByteBuffer b = array.read(offsetLong, shape).getDataAsByteBuffer();
            System.arraycopy(b.array(), 0, buffer, 0, buffer.length);
        } catch (ZarrException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Object read(int[] shape, int[] offset) throws IOException, InvalidRangeException {
        try {
            long[] offsetLong = new long[offset.length];
            for (int i = 0; i < offset.length; i++) {
                offsetLong[i] = offset[i];
            }
            return array.read(offsetLong, shape).copyTo1DJavaArray();
        } catch (ZarrException e) {
            throw new IOException(e);
        }
    }

    @Override
    public int getPixelsType() {
        DataType dataType = array.metadata().dataType();
        switch (dataType) {
            case UINT8:
                return FormatTools.UINT8;
            case INT8:
                return FormatTools.INT8;
            case UINT16:
                return FormatTools.UINT16;
            case INT16:
                return FormatTools.INT16;
            case UINT32:
                return FormatTools.UINT32;
            case INT32:
                return FormatTools.INT32;
            case FLOAT32:
                return FormatTools.FLOAT;
            case FLOAT64:
                return FormatTools.DOUBLE;
            default:
                throw new IllegalArgumentException("Data type " + dataType + " not supported");
        }
    }
}
