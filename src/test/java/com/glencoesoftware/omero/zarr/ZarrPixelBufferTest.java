/*
 * Copyright (C) 2021 Glencoe Software, Inc. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

package com.glencoesoftware.omero.zarr;

import java.awt.Dimension;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import com.glencoesoftware.omero.zarr.ZarrPixelBuffer.Axis;

import com.bc.zarr.ZarrArray;
import com.bc.zarr.ZarrGroup;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.glencoesoftware.bioformats2raw.Converter;

import loci.formats.FormatTools;
import loci.formats.in.FakeReader;
import ome.io.nio.DimensionsOutOfBoundsException;
import ome.model.core.Pixels;
import ome.model.enums.DimensionOrder;
import ome.model.enums.PixelsType;
import ome.util.PixelData;
import picocli.CommandLine;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;

public class ZarrPixelBufferTest {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    public ZarrPixelBuffer createPixelBuffer(
            Pixels pixels, Path path,
            Integer maxPlaneWidth, Integer maxPlaneHeight) throws IOException {
        return new ZarrPixelBuffer(
                pixels, path, maxPlaneWidth, maxPlaneHeight,
                Caffeine.newBuilder()
                    .maximumSize(0)
                    .buildAsync(ZarrPixelsService::getZarrMetadata),
                Caffeine.newBuilder()
                    .maximumSize(0)
                    .buildAsync(ZarrPixelsService::getZarrArray)
                );
    }

    /**
     * Run the bioformats2raw main method and check for success or failure.
     *
     * @param additionalArgs CLI arguments as needed beyond "input output"
     */
    void assertBioFormats2Raw(Path input, Path output, String...additionalArgs)
            throws IOException {
        List<String> args = new ArrayList<String>(
                Arrays.asList(new String[] { "--compression", "null" }));
        for (String arg : additionalArgs) {
            args.add(arg);
        }
        args.add(input.toString());
        args.add(output.toString());
        try {
            Converter converter = new Converter();
            CommandLine cli = new CommandLine(converter);
            cli.execute(args.toArray(new String[]{}));
            Assert.assertTrue(Files.exists(output.resolve(".zattrs")));
            Assert.assertTrue(Files.exists(
                    output.resolve("OME").resolve("METADATA.ome.xml")));
        } catch (RuntimeException rt) {
            throw rt;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    static Path fake(String...args) {
        Assert.assertTrue(args.length %2 == 0);
        Map<String, String> options = new HashMap<String, String>();
        for (int i = 0; i < args.length; i += 2) {
            options.put(args[i], args[i+1]);
        }
        return fake(options);
    }

    static Path fake(Map<String, String> options) {
        return fake(options, null);
    }

    /**
     * Create a Bio-Formats fake INI file to use for testing.
     * @param options map of the options to assign as part of the fake filename
     * from the allowed keys
     * @param series map of the integer series index and options map (same format
     * as <code>options</code> to add to the fake INI content
     * @see https://docs.openmicroscopy.org/bio-formats/6.4.0/developers/
     * generating-test-images.html#key-value-pairs
     * @return path to the fake INI file that has been created
     */
    static Path fake(Map<String, String> options,
            Map<Integer, Map<String, String>> series)
    {
        return fake(options, series, null);
    }

    static Path fake(Map<String, String> options,
            Map<Integer, Map<String, String>> series,
            Map<String, String> originalMetadata)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("image");
        if (options != null) {
            for (Map.Entry<String, String> kv : options.entrySet()) {
                sb.append("&");
                sb.append(kv.getKey());
                sb.append("=");
                sb.append(kv.getValue());
            }
        }
        sb.append("&");
        try {
            List<String> lines = new ArrayList<String>();
            if (originalMetadata != null) {
                lines.add("[GlobalMetadata]");
                for (String key : originalMetadata.keySet()) {
                    lines.add(String.format(
                            "%s=%s", key, originalMetadata.get(key)));
                }
            }
            if (series != null) {
                for (int s : series.keySet()) {
                    Map<String, String> seriesOptions = series.get(s);
                    lines.add(String.format("[series_%d]", s));
                    for (String key : seriesOptions.keySet()) {
                        lines.add(String.format(
                                "%s=%s", key, seriesOptions.get(key)));
                    }
                }
            }
            Path ini = Files.createTempFile(sb.toString(), ".fake.ini");
            File iniAsFile = ini.toFile();
            String iniPath = iniAsFile.getAbsolutePath();
            String fakePath = iniPath.substring(0, iniPath.length() - 4);
            Path fake = Paths.get(fakePath);
            File fakeAsFile = fake.toFile();
            Files.write(fake, new byte[]{});
            Files.write(ini, lines);
            iniAsFile.deleteOnExit();
            fakeAsFile.deleteOnExit();
            return ini;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Path writeTestZarr(
        int sizeT,
        int sizeC,
        int sizeZ,
        int sizeY,
        int sizeX,
        String pixelType,
        int resolutions) throws IOException {

            Path input = fake(
                "sizeT", Integer.toString(sizeT),
                "sizeC", Integer.toString(sizeC),
                "sizeZ", Integer.toString(sizeZ),
                "sizeY", Integer.toString(sizeY),
                "sizeX", Integer.toString(sizeX),
                "pixelType", pixelType,
                "resolutions", Integer.toString(resolutions));
        Path output = tmpDir.getRoot().toPath().resolve("output.zarr");
        assertBioFormats2Raw(input, output);
        
        List<Object> msArray = new ArrayList<>();
        Map<String, Object> msData = new HashMap<>();
        Map<String, Object> msMetadata = new HashMap<>();
        msMetadata.put("method", "loci.common.image.SimpleImageScaler");
        msMetadata.put("version", "Bio-Formats 6.5.1");
        msData.put("metadata", msMetadata);
        msData.put("datasets", getDatasets(resolutions));
        msData.put("version", "0.1");
        msArray.add(msData);
        ZarrGroup z = ZarrGroup.open(output.resolve("0"));
        Map<String,Object> attrs = new HashMap<String, Object>();
        attrs.put("multiscales", msArray);
        z.writeAttributes(attrs);
        return output;
            
        }

    List<Map<String, String>> getDatasets(int resolutions) {
        List<Map<String, String>> datasets = new ArrayList<>();
        for (int i = 0; i < resolutions; i++) {
            Map<String, String> resObj = new HashMap<>();
            resObj.put("path", Integer.toString(i));
            datasets.add(resObj);
        }
        return datasets;
    }

    List<Map<String, String>> getAxes(String order) {
        List<Map<String, String>> axes = new ArrayList<>();
        for (int i = 0; i < order.length(); i++) {
            Map<String, String> axisObj = new HashMap<>();
            axisObj.put("name", Character.toString(order.charAt(i)));
            axes.add(axisObj);
        }
        return axes;
    }

    @Test
    public void testGetChunks() throws IOException {
        int sizeT = 1;
        int sizeC = 3;
        int sizeZ = 1;
        int sizeY = 512;
        int sizeX = 2048;
        int resolutions = 3;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "uint16", resolutions);
        try (ZarrPixelBuffer zpbuf =
                createPixelBuffer(pixels, output.resolve("0"), 1024, 1024)) {
            int[][] chunks = zpbuf.getChunks();
            int[][] expectedChunks = new int[][] {
                new int[] {1, 1, 1, 512, 1024},
                new int[] {1, 1, 1, 256, 1024},
                new int[] {1, 1, 1, 128, 512}
            };
            for(int i = 0; i < chunks.length; i++) {
                Assert.assertTrue(Arrays.equals(
                        chunks[i], expectedChunks[i]));
            }
        }
    }

    @Test
    public void testGetDatasets() throws IOException {
        int sizeT = 1;
        int sizeC = 3;
        int sizeZ = 1;
        int sizeY = 512;
        int sizeX = 2048;
        int resolutions = 3;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "uint16",
                resolutions);
        try (ZarrPixelBuffer zpbuf =
                createPixelBuffer(pixels, output.resolve("0"), 1024, 1024)) {
            List<Map<String,String>> datasets = zpbuf.getDatasets();
            List<Map<String,String>> expectedDatasets = getDatasets(3);
            for (int i = 0; i < datasets.size(); i++) {
                Assert.assertEquals(datasets.get(i), expectedDatasets.get(i));
            }
        }
    }

    @Test
    public void testGetResolutionDescriptions() throws IOException {
        int sizeT = 1;
        int sizeC = 2;
        int sizeZ = 3;
        int sizeY = 512;
        int sizeX = 2048;
        int resolutions = 3;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "uint16", resolutions);
        try (ZarrPixelBuffer zpbuf =
                createPixelBuffer(pixels, output.resolve("0"), 1024, 1024)) {
            List<List<Integer>> expected = new ArrayList<List<Integer>>();
            expected.add(Arrays.asList(new Integer[] {2048, 512}));
            expected.add(Arrays.asList(new Integer[] {1024, 256}));
            expected.add(Arrays.asList(new Integer[] {512, 128}));
            Assert.assertEquals(resolutions, zpbuf.getResolutionLevels());
            Assert.assertEquals(expected, zpbuf.getResolutionDescriptions());

            zpbuf.setResolutionLevel(0);
            Assert.assertEquals(zpbuf.getSizeT(), 1);
            Assert.assertEquals(zpbuf.getSizeC(), 2);
            Assert.assertEquals(zpbuf.getSizeZ(), 3);
            Assert.assertEquals(zpbuf.getSizeY(), 128);
            Assert.assertEquals(zpbuf.getSizeX(), 512);
            zpbuf.setResolutionLevel(1);
            Assert.assertEquals(zpbuf.getSizeT(), 1);
            Assert.assertEquals(zpbuf.getSizeC(), 2);
            Assert.assertEquals(zpbuf.getSizeZ(), 3);
            Assert.assertEquals(zpbuf.getSizeY(), 256);
            Assert.assertEquals(zpbuf.getSizeX(), 1024);
            zpbuf.setResolutionLevel(2);
            Assert.assertEquals(zpbuf.getSizeT(), 1);
            Assert.assertEquals(zpbuf.getSizeC(), 2);
            Assert.assertEquals(zpbuf.getSizeZ(), 3);
            Assert.assertEquals(zpbuf.getSizeY(), 512);
            Assert.assertEquals(zpbuf.getSizeX(), 2048);
        }
    }

    @Test
    public void testGetTile() throws IOException, InvalidRangeException {
        int sizeT = 2;
        int sizeC = 3;
        int sizeZ = 4;
        int sizeY = 5;
        int sizeX = 6;
        int resolutions = 1;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "int32", resolutions);
        ZarrArray test = ZarrArray.open(output.resolve("0").resolve("0"));
        int[] data = new int[2*3*4*5*6];
        for (int i = 0; i < 2*3*4*5*6; i++) {
            data[i] = i;
        }
        test.write(data, new int[] {2,3,4,5,6}, new int[] {0,0,0,0,0});
        try (ZarrPixelBuffer zpbuf =
                createPixelBuffer(pixels, output.resolve("0"), 1024, 1024)) {
            PixelData pixelData = zpbuf.getTile(0, 0, 0, 0, 0, 2, 2);
            ByteBuffer bb = pixelData.getData();
            bb.order(ByteOrder.BIG_ENDIAN);
            IntBuffer ib = bb.asIntBuffer();
            Assert.assertEquals(ib.get(0), 0);
            Assert.assertEquals(ib.get(1), 1);
            Assert.assertEquals(ib.get(2), 6);
            Assert.assertEquals(ib.get(3), 7);
            pixelData = zpbuf.getTile(1, 1, 1, 1, 1, 2, 2);
            bb = pixelData.getData();
            bb.order(ByteOrder.BIG_ENDIAN);
            ib = bb.asIntBuffer();
            Assert.assertEquals(ib.get(0), 517);//360(6*5*4*3) + 120(6*5*4) + 30(6*5) + 6 + 1
            Assert.assertEquals(ib.get(1), 518);
            Assert.assertEquals(ib.get(2), 523);
            Assert.assertEquals(ib.get(3), 524);
        }
    }

    private Array asArray(byte[] storage, int[] shape) {
        // XXX: Is not data type agnostic, expects signed 32-bit integer pixels
        int size = IntStream.of(shape).reduce(1, Math::multiplyExact);
        int[] asInt = new int[size];
        ByteBuffer.wrap(storage).asIntBuffer().get(asInt);
        return Array.factory(DataType.INT, shape, asInt);
    }

    private byte[] getStack(
            byte[] timepoint, int c, int sizeC, int sizeZ, int sizeX,
            int sizeY) {
        return getStack(timepoint, c, sizeC, sizeZ, sizeX, sizeY, "TCZYX");
    }

    private byte[] getStack(
            byte[] timepoint, int c, int sizeC, int sizeZ, int sizeX,
            int sizeY, String order) {
        // XXX: Is not data type agnostic, expects signed 32-bit integer pixels
        int bytesPerPixel = 4;
        int[] shape = new int[4];
        String shapeorder = order.replace("T", "");
        shape[shapeorder.indexOf('C')] = sizeC;
        shape[shapeorder.indexOf('Z')] = sizeZ;
        shape[shapeorder.indexOf('Y')] = sizeY;
        shape[shapeorder.indexOf('X')] = sizeX;
        int size = IntStream.of(new int[] {sizeZ, sizeY, sizeX, bytesPerPixel})
                .reduce(1, Math::multiplyExact);
        Array array = asArray(timepoint, shape).slice(shapeorder.indexOf('C'), c);
        byte[] asBytes = new byte[size];
        ByteBuffer.wrap(asBytes).asIntBuffer()
                .put((int[]) array.copyTo1DJavaArray());
        return asBytes;
    }

    private byte[] getPlane(
            byte[] stack, int z, int sizeZ, int sizeX, int sizeY) {
        return getPlane(stack, z, sizeZ, sizeX, sizeY, "TCZYX");
    }

    private byte[] getPlane(
            byte[] stack, int z, int sizeZ, int sizeX, int sizeY, String order) {
        // XXX: Is not data type agnostic, expects signed 32-bit integer pixels
        int bytesPerPixel = 4;
        String shapeorder = order.replace("T", "").replace("C", "");
        int[] shape = new int[3];
        shape[shapeorder.indexOf('Z')] = sizeZ;
        shape[shapeorder.indexOf('Y')] = sizeY;
        shape[shapeorder.indexOf('X')] = sizeX;
        int size = IntStream.of(new int[] {sizeY, sizeX, bytesPerPixel})
                .reduce(1, Math::multiplyExact);
        Array array = asArray(stack, shape).slice(shapeorder.indexOf('Z'), z);
        byte[] asBytes = new byte[size];
        ByteBuffer.wrap(asBytes).asIntBuffer()
                .put((int[]) array.copyTo1DJavaArray());
        return asBytes;
    }

    private byte[] getCol(byte[] plane, int x, int sizeX, int sizeY) {
        // XXX: Is not data type agnostic, expects signed 32-bit integer pixels
        int bytesPerPixel = 4;
        int[] shape = new int[] {sizeY, sizeX};
        int size = IntStream.of(new int[] {sizeY, bytesPerPixel})
                .reduce(1, Math::multiplyExact);
        Array array = asArray(plane, shape).slice(1, x);
        byte[] asBytes = new byte[size];
        ByteBuffer.wrap(asBytes).asIntBuffer()
                .put((int[]) array.copyTo1DJavaArray());
        return asBytes;
    }

    @Test
    public void testGetTimepointStackPlaneRowCol()
            throws IOException, InvalidRangeException {
        int sizeT = 2;
        int sizeC = 3;
        int sizeZ = 4;
        int sizeY = 1024;
        int sizeX = 2048;
        int resolutions = 1;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "int32", resolutions);
        try (ZarrPixelBuffer zpbuf =
                createPixelBuffer(pixels, output.resolve("0"), 2048, 2048)) {
            for (int t = 0; t < sizeT; t++) {
                // Assert timepoint
                byte[] timepoint = zpbuf.getTimepoint(t).getData().array();
                for (int c = 0; c < sizeC; c++) {
                    // Assert stack
                    byte[] stack = zpbuf.getStack(c, t).getData().array();
                    byte[] stackFromTimepoint =
                            getStack(timepoint, c, sizeC, sizeZ, sizeX, sizeY);
                    Assert.assertArrayEquals(stack, stackFromTimepoint);
                    for (int z = 0; z < sizeZ; z++) {
                        // Assert plane
                        byte[] plane =
                            zpbuf.getPlane(z, c, t).getData().array();
                        byte[] planeFromStack =
                                getPlane(stack, z, sizeZ, sizeX, sizeY);
                        Assert.assertArrayEquals(plane, planeFromStack);
                        int[] seriesPlaneNumberZCT =
                            FakeReader.readSpecialPixels(
                                    plane, zpbuf.getPixelsType(), false);
                        int planeNumber = FormatTools.getIndex(
                                DimensionOrder.VALUE_XYZCT,
                                sizeZ, sizeC, sizeT, sizeZ * sizeC * sizeT,
                                z, c, t);
                        Assert.assertArrayEquals(
                                Arrays.toString(seriesPlaneNumberZCT),
                                new int[] {0, planeNumber, z, c, t},
                                seriesPlaneNumberZCT);
                        // Assert row
                        int y = sizeY / 2;
                        int rowSize = zpbuf.getRowSize();
                        int rowOffset = y * rowSize;
                        byte[] row = zpbuf.getRow(y, z, c, t).getData().array();
                        byte[] rowExpected = Arrays.copyOfRange(
                                plane, rowOffset, rowOffset + rowSize);
                        Assert.assertArrayEquals(rowExpected, row);
                        // Assert column
                        int x = sizeX / 2;
                        byte[] col = zpbuf.getCol(x, z, c, t).getData().array();
                        byte[] colExpected = getCol(plane, x, sizeX, sizeY);
                        Assert.assertArrayEquals(colExpected, col);
                    }
                }
            }
        }
    }

    @Test(expected = DimensionsOutOfBoundsException.class)
    public void testGetTileLargerThanImage()
            throws IOException, InvalidRangeException {
        int sizeT = 2;
        int sizeC = 3;
        int sizeZ = 4;
        int sizeY = 5;
        int sizeX = 6;
        int resolutions = 1;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "int32",
                resolutions);
        ZarrArray test = ZarrArray.open(output.resolve("0").resolve("0"));
        int[] data = new int[2*3*4*5*6];
        for (int i = 0; i < 2*3*4*5*6; i++) {
            data[i] = i;
        }
        test.write(data, new int[] {2,3,4,5,6}, new int[] {0,0,0,0,0});
        try (ZarrPixelBuffer zpbuf =
                createPixelBuffer(pixels, output.resolve("0"), 1024, 1024)) {
            zpbuf.setResolutionLevel(0);
            PixelData pixelData = zpbuf.getTile(0, 0, 0, 0, 0, 10, 10);
            ByteBuffer bb = pixelData.getData();
            bb.order(ByteOrder.BIG_ENDIAN);
            IntBuffer ib = bb.asIntBuffer();
            Assert.assertEquals(ib.get(0), 0);
            Assert.assertEquals(ib.get(1), 1);
            Assert.assertEquals(ib.get(2), 6);
            Assert.assertEquals(ib.get(3), 7);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTileIntegerOverflow()
            throws IOException, InvalidRangeException {
        int sizeT = 1;
        int sizeC = 3;
        int sizeZ = 1;
        int sizeY = 1;
        int sizeX = 1;
        int resolutions = 1;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "uint16",
                resolutions);

        // Hack the .zarray so we can appear as though we have more data than
        // we actually have written above.
        ObjectMapper mapper = new ObjectMapper();
        HashMap<String, Object> zArray = mapper.readValue(
                Files.readAllBytes(output.resolve("0/0/.zarray")),
                HashMap.class);
        List<Integer> shape = (List<Integer>) zArray.get("shape");
        shape.set(3, 50000);
        shape.set(4, 50000);
        mapper.writeValue(output.resolve("0/0/.zarray").toFile(), zArray);
        try (ZarrPixelBuffer zpbuf =
                createPixelBuffer(pixels, output.resolve("0"), 32, 32)) {
            zpbuf.getTile(0, 0, 0, 0, 0, 50000, 50000);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTileExceedsMinMax() throws IOException {
        int sizeT = 1;
        int sizeC = 3;
        int sizeZ = 1;
        int sizeY = 512;
        int sizeX = 2048;
        int resolutions = 3;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "uint16",
                resolutions);

        try (ZarrPixelBuffer zpbuf =
                createPixelBuffer(pixels, output.resolve("0"), 32, 32)) {
            Assert.assertNull(zpbuf.getTile(0, 0, 0, 0, 0, 32, 33));
            // Throws exception
            zpbuf.getTile(0, 0, 0, -1, 0, 1, 1);
        }
    }

    @Test
    public void testCheckBoundsValidZeros() throws IOException {
        int sizeT = 1;
        int sizeC = 2;
        int sizeZ = 3;
        int sizeY = 512;
        int sizeX = 2048;
        int resolutions = 3;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "uint16", resolutions);
        try (ZarrPixelBuffer zpbuf =
                createPixelBuffer(pixels, output.resolve("0"), 1024, 1024)) {
            zpbuf.checkBounds(0, 0, 0, 0, 0);
        }
    }

    @Test
    public void testCheckBoundsValidEnd() throws IOException {
        int sizeT = 1;
        int sizeC = 2;
        int sizeZ = 3;
        int sizeY = 512;
        int sizeX = 2048;
        int resolutions = 3;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "uint16", resolutions);
        try (ZarrPixelBuffer zpbuf =
                createPixelBuffer(pixels, output.resolve("0"), 1024, 1024)) {
            zpbuf.checkBounds(2047, 511, 2, 1, 0);
        }
    }

    @Test(expected = DimensionsOutOfBoundsException.class)
    public void testCheckBoundsOutOfRange() throws IOException {
        int sizeT = 1;
        int sizeC = 2;
        int sizeZ = 3;
        int sizeY = 512;
        int sizeX = 2048;
        int resolutions = 3;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "uint16", resolutions);
        try (ZarrPixelBuffer zpbuf =
                createPixelBuffer(pixels, output.resolve("0"), 1024, 1024)) {
            zpbuf.checkBounds(2048, 511, 2, 1, 0);
        }
    }

    @Test(expected = DimensionsOutOfBoundsException.class)
    public void testCheckBounds() throws IOException {
        int sizeT = 1;
        int sizeC = 2;
        int sizeZ = 3;
        int sizeY = 512;
        int sizeX = 2048;
        int resolutions = 3;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "uint16", resolutions);
        try (ZarrPixelBuffer zpbuf =
                createPixelBuffer(pixels, output.resolve("0"), 1024, 1024)) {
            zpbuf.checkBounds(-1, 0, 0, 0, 0);
        }
    }

    @Test
    public void testGetTileSize() throws IOException {
        int sizeT = 1;
        int sizeC = 1;
        int sizeZ = 1;
        int sizeY = 2048;
        int sizeX = 2048;
        int resolutions = 3;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "uint16", resolutions);
        try (ZarrPixelBuffer zpbuf =
                createPixelBuffer(pixels, output.resolve("0"), 1024, 1024)) {
            Dimension tileSize = zpbuf.getTileSize();
            Assert.assertEquals(1024, tileSize.getWidth(), 0.1);
            Assert.assertEquals(1024, tileSize.getHeight(), 0.1);
        }
    }

    @Test
    public void testUint16() throws IOException {
        int sizeT = 1;
        int sizeC = 1;
        int sizeZ = 1;
        int sizeY = 2048;
        int sizeX = 2048;
        int resolutions = 3;
        int bytesPerPixel = 2;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "uint16", resolutions);
        try (ZarrPixelBuffer zpbuf =
                createPixelBuffer(pixels, output.resolve("0"), 1024, 1024)) {
            Assert.assertEquals(FormatTools.UINT16, zpbuf.getPixelsType());
            Assert.assertEquals(false, zpbuf.isSigned());
            Assert.assertEquals(false, zpbuf.isFloat());
            Assert.assertEquals(bytesPerPixel, zpbuf.getByteWidth());
        }
    }

    @Test
    public void testFloat() throws IOException {
        int sizeT = 1;
        int sizeC = 1;
        int sizeZ = 1;
        int sizeY = 2048;
        int sizeX = 2048;
        int resolutions = 3;
        int bytesPerPixel = 4;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "float", resolutions);
        try (ZarrPixelBuffer zpbuf =
                createPixelBuffer(pixels, output.resolve("0"), 1024, 1024)) {
            Assert.assertEquals(FormatTools.FLOAT, zpbuf.getPixelsType());
            Assert.assertEquals(true, zpbuf.isSigned());
            Assert.assertEquals(true, zpbuf.isFloat());
            Assert.assertEquals(bytesPerPixel, zpbuf.getByteWidth());
        }
    }

    @Test
    public void testSizes() throws IOException {
        int sizeT = 4;
        int sizeC = 2;
        int sizeZ = 3;
        int sizeY = 1024;
        int sizeX = 2048;
        int resolutions = 3;
        int bytesPerPixel = 2;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "uint16", resolutions);
        try (ZarrPixelBuffer zpbuf =
                createPixelBuffer(pixels, output.resolve("0"), 1024, 1024)) {
            // Plane size
            Assert.assertEquals(
                    sizeX * sizeY * bytesPerPixel,
                    zpbuf.getPlaneSize().longValue());
            // Stack size
            Assert.assertEquals(
                    sizeZ * sizeX * sizeY * bytesPerPixel,
                    zpbuf.getStackSize().longValue());
            // Timepoint size
            Assert.assertEquals(
                    sizeC * sizeZ * sizeX * sizeY * bytesPerPixel,
                    zpbuf.getTimepointSize().longValue());
            // Total size
            Assert.assertEquals(
                    sizeT * sizeC * sizeZ * sizeX * sizeY * bytesPerPixel,
                    zpbuf.getTotalSize().longValue());
            // Column size
            Assert.assertEquals(
                    sizeY * bytesPerPixel,
                    zpbuf.getColSize().longValue());
            // Row size
            Assert.assertEquals(
                    sizeX * bytesPerPixel,
                    zpbuf.getRowSize().longValue());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetResolutionLevelOutOfBounds() throws IOException {
        int sizeT = 1;
        int sizeC = 1;
        int sizeZ = 1;
        int sizeY = 2048;
        int sizeX = 2048;
        int resolutions = 3;
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "uint16", resolutions);
        try (ZarrPixelBuffer zpbuf =
                createPixelBuffer(pixels, output.resolve("0"), 1024, 1024)) {
            zpbuf.setResolutionLevel(3);
        }
    }

    @Test
    public void testDownsampledZ() throws IOException {
        int sizeT = 1;
        int sizeC = 1;
        int sizeZ = 16;
        int sizeY = 2048;
        int sizeX = 2048;
        int resolutions = 3;

        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "uint8", resolutions);

        // Hack the .zarray to hide Z sections in lower resolutions
        for (int r=1; r<resolutions; r++) {
            ObjectMapper mapper = new ObjectMapper();
            HashMap<String, Object> zArray = mapper.readValue(
                    Files.readAllBytes(output.resolve("0/" + r + "/.zarray")),
                    HashMap.class);
            List<Integer> shape = (List<Integer>) zArray.get("shape");
            shape.set(2, sizeZ / (int) Math.pow(2, r));
            mapper.writeValue(output.resolve("0/" + r + "/.zarray").toFile(), zArray);
        }

        try (ZarrPixelBuffer zpbuf =
                createPixelBuffer(pixels, output.resolve("0"), sizeX, sizeY)) {
            // get the last Z section, for each resolution level
            for (int r=0; r<resolutions; r++) {
                zpbuf.setResolutionLevel(r);

                byte[] plane = zpbuf.getPlane(sizeZ - 1, 0, 0).getData().array();
            }
        }
    }

    @Test
    public void testReadDataNonDefaultAxes()
            throws IOException, InvalidRangeException {
        // Pretty much the same as testGetTimepointStackPlaneRowCol()
        // but testing a different axes order.
        int sizeT = 2;
        int sizeC = 3;
        int sizeZ = 4;
        int sizeY = 1024;
        int sizeX = 2048;
        String order = DimensionOrder.VALUE_XYCTZ; // Default XYZCT
        String rev_order  = new StringBuilder(order).reverse().toString();
        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", new DimensionOrder(order));

        Path output = tmpDir.getRoot().toPath().resolve("test.zarr");
        new TestZarr()
                .setPath(output)
                .setOverwrite(true)
                .setSizeX(sizeX)
                .setSizeY(sizeY)
                .setSizeZ(sizeZ)
                .setSizeT(sizeT)
                .setSizeC(sizeC)
                .setOrder(rev_order)
                .setDataType(com.bc.zarr.DataType.i4) // getStack expects int32
                .init()
                .createImage()
                .createMetadata();

        try (ZarrPixelBuffer zpbuf =
                createPixelBuffer(pixels, output.resolve("0"), sizeX, sizeY)) {
            for (int t = 0; t < sizeT; t++) {
                // Assert timepoint
                byte[] timepoint = zpbuf.getTimepoint(t).getData().array();
                for (int c = 0; c < sizeC; c++) {
                    // Assert stack
                    byte[] stack = zpbuf.getStack(c, t).getData().array();
                    byte[] stackFromTimepoint =
                            getStack(timepoint, c, sizeC, sizeZ, sizeX, sizeY, rev_order);
                    Assert.assertArrayEquals(stack, stackFromTimepoint);
                    for (int z = 0; z < sizeZ; z++) {
                        // Assert plane
                        byte[] plane =
                            zpbuf.getPlane(z, c, t).getData().array();
                        byte[] planeFromStack =
                                getPlane(stack, z, sizeZ, sizeX, sizeY, rev_order);
                        Assert.assertArrayEquals(plane, planeFromStack);
                        // Assert row
                        int y = sizeY / 2;
                        int rowSize = zpbuf.getRowSize();
                        int rowOffset = y * rowSize;
                        byte[] row = zpbuf.getRow(y, z, c, t).getData().array();
                        byte[] rowExpected = Arrays.copyOfRange(
                                plane, rowOffset, rowOffset + rowSize);
                        Assert.assertArrayEquals(rowExpected, row);
                        // Assert column
                        int x = sizeX / 2;
                        byte[] col = zpbuf.getCol(x, z, c, t).getData().array();
                        byte[] colExpected = getCol(plane, x, sizeX, sizeY);
                        Assert.assertArrayEquals(colExpected, col);
                    }
                }
            }
        }
    }

    @Test
    public void test_default_order()
        throws IOException, InvalidRangeException {
        // Check that if access are not in the file it defaults to TCZYX order when no axes found

        int sizeT = 1;
        int sizeC = 2;
        int sizeZ = 16;
        int sizeY = 256;
        int sizeX = 512;
        int resolutions = 1;

        Pixels pixels = new Pixels(
                null, null, sizeX, sizeY, sizeZ, sizeC, sizeT, "", null);
        Path output = writeTestZarr(
                sizeT, sizeC, sizeZ, sizeY, sizeX, "uint8", resolutions);

        try (ZarrPixelBuffer zpbuf =
                createPixelBuffer(pixels, output.resolve("0"), sizeX, sizeY)) {
                    Map<Axis, Integer> axes = zpbuf.getAxesOrder();
                    Assert.assertEquals(0, axes.get(Axis.T).intValue());
                    Assert.assertEquals(1, axes.get(Axis.C).intValue());
                    Assert.assertEquals(2, axes.get(Axis.Z).intValue());
                    Assert.assertEquals(3, axes.get(Axis.Y).intValue());
                    Assert.assertEquals(4, axes.get(Axis.X).intValue());
                    Assert.assertEquals(sizeT, zpbuf.getSizeT());
                    Assert.assertEquals(sizeC, zpbuf.getSizeC());
                    Assert.assertEquals(sizeZ, zpbuf.getSizeZ());
                    Assert.assertEquals(sizeY, zpbuf.getSizeY());
                    Assert.assertEquals(sizeX, zpbuf.getSizeX());
        }
    }

    @Test
    public void test_XYCTZ() throws IOException, InvalidRangeException {
        testDimensions(512, 1024, 2, 3, 4);
    }

    @Test
    public void test_XYCT() throws IOException, InvalidRangeException {
        testDimensions(512, 1024, 0, 3, 4);
    }

    @Test
    public void test_XYCZ() throws IOException, InvalidRangeException {
        testDimensions(512, 1024, 2, 3, 0);
    }

    @Test
    public void test_XYTZ() throws IOException, InvalidRangeException {
        testDimensions(512, 1024, 2, 0, 4);
    }

    @Test
    public void test_XYZ() throws IOException, InvalidRangeException {
        testDimensions(512, 1024, 2, 0, 0);
    }

    @Test
    public void test_XYT() throws IOException, InvalidRangeException {
        testDimensions(512, 1024, 0, 0, 4);
    }

    @Test
    public void test_XYC() throws IOException, InvalidRangeException {
        testDimensions(512, 1024, 0, 3, 0);
    }

    @Test
    public void test_XY() throws IOException, InvalidRangeException {
        testDimensions(512, 1024, 0, 0, 0);
    }

    @Test
    public void test_Order_XYCTZ() throws IOException, InvalidRangeException {
        testOrder(DimensionOrder.VALUE_XYCTZ);
    }

    @Test
    public void test_Order_XYCZT() throws IOException, InvalidRangeException {
        testOrder(DimensionOrder.VALUE_XYCZT);
    }

    @Test
    public void test_Order_XYTCZ() throws IOException, InvalidRangeException {
        testOrder(DimensionOrder.VALUE_XYTCZ);
    }

    @Test
    public void test_Order_XYTZC() throws IOException, InvalidRangeException {
        testOrder(DimensionOrder.VALUE_XYTZC);
    }

    @Test
    public void test_Order_XYZCT() throws IOException, InvalidRangeException {
        testOrder(DimensionOrder.VALUE_XYZCT);
    }

    @Test
    public void test_Order_XYZTC() throws IOException, InvalidRangeException {
        testOrder(DimensionOrder.VALUE_XYZTC);
    }

    private void testOrder(String order) throws IOException, InvalidRangeException {
        Path testZarrPath = tmpDir.getRoot().toPath().resolve("test.zarr");
        TestZarr testZarr = new TestZarr()
            .setPath(testZarrPath)
            .setOrder(new StringBuilder(order).reverse().toString())
            .init()
            .createImage()
            .createMetadata();

        Pixels pixels = new Pixels(
            null, null, testZarr.getSizeX(), testZarr.getSizeY(), testZarr.getSizeZ(), testZarr.getSizeC(), testZarr.getSizeT(), "", new DimensionOrder(order));

        try (ZarrPixelBuffer zpbuf =
            createPixelBuffer(pixels, testZarrPath.resolve("0"), testZarr.getSizeX(), testZarr.getSizeY())) {
                Map<Axis, Integer> axes = zpbuf.getAxesOrder();
                Assert.assertEquals(4 - order.indexOf("Z"), axes.get(Axis.Z).intValue());
                Assert.assertEquals(4 - order.indexOf("T"), axes.get(Axis.T).intValue());
                Assert.assertEquals(4 - order.indexOf("C"), axes.get(Axis.C).intValue());
                Assert.assertEquals(4 - order.indexOf("Y"), axes.get(Axis.Y).intValue());
                Assert.assertEquals(4 - order.indexOf("X"), axes.get(Axis.X).intValue());
                Assert.assertEquals(testZarr.getSizeT(), zpbuf.getSizeT());
                Assert.assertEquals(testZarr.getSizeC(), zpbuf.getSizeC());
                Assert.assertEquals(testZarr.getSizeZ(), zpbuf.getSizeZ());
                Assert.assertEquals(testZarr.getSizeY(), zpbuf.getSizeY());
                Assert.assertEquals(testZarr.getSizeX(), zpbuf.getSizeX());
        }
    }

    private void testDimensions(int sizeX, int sizeY, int sizeZ, int sizeC, int sizeT) throws IOException, InvalidRangeException {
        int textX = 10;
        int textY = 10;
        
        String order = DimensionOrder.VALUE_XYZCT;
        if (sizeT == 0) {
            order.replace("T", "");
        }
        if (sizeZ == 0) {
            order.replace("Z", "");
        }
        if (sizeC == 0) {
            order.replace("C", "");
        }
        
        Path testZarrPath = tmpDir.getRoot().toPath().resolve("test.zarr");
        TestZarr testZarr = new TestZarr()
            .setPath(testZarrPath)
            .setOverwrite(true)
            .setSizeX(sizeX)
            .setSizeY(sizeY)
            .setSizeZ(sizeZ)
            .setSizeT(sizeT)
            .setSizeC(sizeC)
            .setOrder(new StringBuilder(order).reverse().toString())
            .setTextX(textX)
            .setTextY(textY)
            .init()
            .createImage()
            .createMetadata();

        int pixZ = sizeZ > 0 ? sizeZ : 1;
        int pixC = sizeC > 0 ? sizeC : 1;
        int pixT = sizeT > 0 ? sizeT : 1;
        
        Pixels pixels = new Pixels(
            null, new PixelsType(PixelsType.VALUE_INT32), 
            sizeX, sizeY, pixZ, pixC, pixT, "", new DimensionOrder(DimensionOrder.VALUE_XYZCT));
        
        int expectedTests = pixC * pixT * pixZ;
        int testCount = 0;
        try (ZarrPixelBuffer zpbuf = createPixelBuffer(pixels, testZarrPath.resolve("0"), sizeX, sizeY)) {
            for (int t = 0; t <= sizeT; t++) {
                if (t == sizeT && sizeT > 0)
                    break;
                for (int z = 0; z <= sizeZ; z++) {
                    if (z == sizeZ && sizeZ > 0)
                        break;
                    for (int c = 0; c <= sizeC; c++) {
                        if (c == sizeC && sizeC > 0)
                            break;
                        byte[] expected = testZarr.generateGreyscaleImageWithText(c, z, t);
                        byte[] actual = zpbuf.getPlane(z, c, t).getData().array();
                        Assert.assertArrayEquals(expected, actual);
                        testCount++;
                    }
                }
            }
        }   
        Assert.assertEquals(expectedTests, testCount);
    }
}
