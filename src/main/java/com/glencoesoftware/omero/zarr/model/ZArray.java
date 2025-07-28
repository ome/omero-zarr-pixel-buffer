package com.glencoesoftware.omero.zarr.model;

import java.io.IOException;

import ucar.ma2.InvalidRangeException;

public interface ZArray {
    public int[] getShape();

    public int[] getChunks();

    public void read(byte[] buffer, int[] shape, int[] offset) throws IOException, InvalidRangeException;

    public Object read(int[] shape, int[] offset) throws IOException, InvalidRangeException;

    public int getPixelsType();

}
