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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import ome.model.core.Pixels;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Unit tests for ZarrPixelBuffer. */
public class ZarrPixelBufferTest {

    private static final String[] SAMPLES = {
        "https://github.com/dominikl/utils/raw/refs/heads/zarr_test_images/omero_zarr/test_images/test_04.zarr.tgz",
        "https://github.com/dominikl/utils/raw/refs/heads/zarr_test_images/omero_zarr/test_images/test_05.zarr.tgz" };

    private static final int size_x = 512;
    private static final int size_y = 256;
    private static final int size_c = 3;
    private static final int size_z = 4;
    private static final int size_t = 5;
    private static final ExpectedValueFunction expectedValueFunction = (x, y, c, z, t) -> x + 1
        + 1000 * (y + 1) + 1000000 * (c + 1) + 10000000 * (z + 1) + 100000000 * (t + 1);

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    /**
     * Tests that all configured sample Zarr archives can be read and that their
     * pixel data matches the expected pattern.
     *
     * @throws IOException if a sample cannot be downloaded, extracted or read
     */
    @Test
    public void testSamples() throws IOException {
        for (String sample : SAMPLES) {
            test(sample);
        }
    }

    /**
     * Tests a single Zarr sample archive by downloading it, opening the
     * corresponding ZarrPixelBuffer and verifying all planes.
     *
     * @param sample URL of the sample Zarr archive
     * @throws IOException if the sample cannot be downloaded, extracted or read
     */
    public void test(String sample) throws IOException {
        String name = getSample(sample, tmpDir.getRoot().toPath());
        System.out.println("Testing: " + name);
        Path path = tmpDir.getRoot().toPath().resolve(name);
        Pixels pixels = new Pixels(null, null, size_x, size_y, size_z, size_c, size_t, "", null);
        ZarrPixelBuffer zb = createPixelBuffer(pixels, path, size_x, size_y);
        int byteWidth = zb.getByteWidth();
        assertEquals(4, byteWidth);
        for (int t = 0; t < size_t; t++) {
            for (int c = 0; c < size_c; c++) {
                for (int z = 0; z < size_z; z++) {
                    byte[] plane = zb.getPlane(z, c, t).getData().array();
                    byte[] expected = getExpected(c, z, t);
                    assertArrayEquals(expected, plane);
                }
            }
        }
    }

    /**
     * Computes the expected plane pixel values for the given indices.
     *
     * @param c channel index
     * @param z z-section index
     * @param t timepoint index
     * @return a byte array containing the expected plane values
     */
    private byte[] getExpected(int c, int z, int t) {
        ByteBuffer buf = ByteBuffer.allocate(size_x * size_y * 4);
        for (int y = 0; y < size_y; y++) {
            for (int x = 0; x < size_x; x++) {
                Integer value = expectedValueFunction.apply(x, y, c, z, t);
                buf.putInt(value);
            }
        }
        return buf.array();
    }

    /**
     * Downloads and extracts a Zarr sample archive into the given destination
     * directory.
     *
     * @param url URL of the sample archive to download
     * @param dest destination directory where the archive should be extracted
     * @return the name of the top-level directory created by extracting the archive
     * @throws IOException if the archive cannot be downloaded or extracted
     */
    private String getSample(String url, Path dest) throws IOException {
        Files.createDirectories(dest);

        Path tmpTarGz = Files.createTempFile(tmpDir.getRoot().toPath(), "sample", ".tgz");
        try (InputStream in = new URL(url).openStream()) {
            Files.copy(in, tmpTarGz, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }

        try (InputStream fin = Files.newInputStream(tmpTarGz);
                GzipCompressorInputStream gin =
                        new GzipCompressorInputStream(fin);
                TarArchiveInputStream tin = new TarArchiveInputStream(gin)) {
            TarArchiveEntry entry;
            while ((entry = tin.getNextEntry()) != null) {
                Path outPath = dest.resolve(entry.getName()).normalize();
                if (!outPath.startsWith(dest)) {
                    throw new IOException("Entry is outside of the target dir: " + entry.getName());
                }
                if (entry.isDirectory()) {
                    Files.createDirectories(outPath);
                } else {
                    if (outPath.getParent() != null) {
                        Files.createDirectories(outPath.getParent());
                    }
                    Files.copy(tin, outPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                }
            }
            String name = url.substring(url.lastIndexOf('/') + 1);
            return name.replace(".tgz", "");
        } finally {
            Files.deleteIfExists(tmpTarGz);
        }
    }

    /** Create a ZarrPixelBuffer instance for the given sample. */
    private ZarrPixelBuffer createPixelBuffer(
            Pixels pixels, Path path,
            Integer maxPlaneWidth, Integer maxPlaneHeight) throws IOException {
        ZarrStore root;
        try {
            root = new ZarrStore(path.toString());
        } catch (Exception e) {
            throw new IOException(e);
        }
        return new ZarrPixelBuffer(
                pixels, root, maxPlaneWidth, maxPlaneHeight,
                Caffeine.newBuilder()
                    .maximumSize(0)
                    .buildAsync(ZarrPixelsService::getZarrMetadata),
                Caffeine.newBuilder()
                    .maximumSize(0)
                    .buildAsync(ZarrPixelsService::getZarrArray)
                );
    }

    /**
     * Functional interface used to compute the expected integer pixel value
     * from image coordinates and indices.
     */
    @FunctionalInterface
    private interface ExpectedValueFunction {
        int apply(int x, int y, int c, int z, int t);
    }
}
