package com.glencoesoftware.omero.zarr.compat;

import java.io.IOException;
import ucar.ma2.InvalidRangeException;

/**
 * Interface for representing a Zarr array with methods for accessing array properties and reading
 * data from the array.
 */
public interface ZArray {

    /**
     * Gets the shape (dimensions) of the array.
     *
     * @return an array of integers representing the size of each dimension
     */
    public int[] getShape();

    /**
     * Gets the chunk size.
     *
     * @return an array of integers representing the chunk size.
     */
    public int[] getChunks();

    /**
     * Reads data from the array into a provided byte buffer.
     *
     * @param buffer the byte buffer to read data into
     * @param shape  the shape of the data to read
     * @param offset the offset position to start reading from
     * @throws IOException           if an I/O error occurs during reading
     * @throws InvalidRangeException if the specified range is invalid
     */
    public void read(byte[] buffer, int[] shape, int[] offset)
        throws IOException, InvalidRangeException;

    /**
     * Reads data from the array and returns it as an Object (short[], int[], etc.).
     *
     * @param shape  the shape of the data to read
     * @param offset the offset position to start reading from
     * @return the data read from the array as an Object
     * @throws IOException           if an I/O error occurs during reading
     * @throws InvalidRangeException if the specified range is invalid
     */
    public Object read(int[] shape, int[] offset) throws IOException, InvalidRangeException;

    /**
     * Gets the pixel type identifier for this array.
     *
     * @return an integer representing the pixel type
     */
    public int getPixelsType();

}
