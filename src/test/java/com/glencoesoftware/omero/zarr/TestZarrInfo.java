package com.glencoesoftware.omero.zarr;

import com.glencoesoftware.omero.zarr.compat.ZarrInfo;
import com.glencoesoftware.omero.zarr.compat.ZarrInfo.StorageType;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test ZarrInfo.
 */
public class TestZarrInfo {

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    /**
     * Test local storage, zarr v2.
     *
     * @throws IOException If an I/O error occurs.
     */
    @Test
    public void testLocalV2() throws IOException {
        String path = writeTestZarr(1, 1, 1, 256, 256, "uint8", 1).toString() + "/0";

        ZarrInfo zp = new ZarrInfo(path);
        System.out.println("testLocalV2: " + zp);
        Assert.assertEquals(zp.getStorageType(), StorageType.FILE);
        Assert.assertEquals(new ComparableVersion("2"), zp.getZarrVersion());
        Assert.assertEquals(new ComparableVersion("0.4"), zp.getNgffVersion());
    }

    /**
     * Test local storage, zarr v3.
     *
     * @throws IOException If an I/O error occurs.
     */
    @Test
    public void testLocalV3() throws IOException {
        // TODO: implement
        // String path = writeTestZarr(1, 1, 1, 256, 256, "uint8", 1).toString() + "/0";
        // ZarrInfo zp = new ZarrInfo(path);
        // Assert.assertEquals(zp.getStorageType(), StorageType.FILE);
        // Assert.assertEquals(new ComparableVersion("3"), zp.getZarrVersion());
        // Assert.assertEquals(new ComparableVersion("0.5"), zp.getNgffVersion());
    }

    /**
     * Test HTTP storage, zarr v2.
     *
     * @throws IOException If an I/O error occurs.
     */
    @Test
    public void testHTTPV2() throws IOException {
        ZarrInfo zp = new ZarrInfo("https://s3.ltd.ovh/public/cat_z2.ome.zarr/0");
        System.out.println("testHTTPV2: " + zp);
        Assert.assertEquals(zp.getStorageType(), StorageType.HTTP);
        Assert.assertEquals(new ComparableVersion("2"), zp.getZarrVersion());
        Assert.assertEquals(new ComparableVersion("0.4"), zp.getNgffVersion());
    }

    /**
     * Test HTTP storage, zarr v3.
     *
     * @throws IOException If an I/O error occurs.
     */
    @Test
    public void testHTTPV3() throws IOException {
        ZarrInfo zp = new ZarrInfo("https://s3.ltd.ovh/public/cat_z3.ome.zarr/0");
        System.out.println("testHTTPV3: " + zp);
        Assert.assertEquals(zp.getStorageType(), StorageType.HTTP);
        Assert.assertEquals(new ComparableVersion("3"), zp.getZarrVersion());
        Assert.assertEquals(new ComparableVersion("0.5"), zp.getNgffVersion());
    }

    /**
     * Test public S3 storage, zarr v2.
     *
     * @throws IOException If an I/O error occurs.
     */
    @Test
    public void testS3V2Public() throws IOException {
        ZarrInfo zp = new ZarrInfo("s3://s3.ltd.ovh/public/cat_z2.ome.zarr/0?anonymous=true");
        System.out.println("testS3V2Public: " + zp);
        Assert.assertEquals(zp.getStorageType(), StorageType.S3);
        Assert.assertEquals(new ComparableVersion("2"), zp.getZarrVersion());
        Assert.assertEquals(new ComparableVersion("0.4"), zp.getNgffVersion());
    }

    /**
     * Test public S3 storage, zarr v3.
     *
     * @throws IOException If an I/O error occurs.
     */
    @Test
    public void testS3V3Public() throws IOException {
        ZarrInfo zp = new ZarrInfo("s3://s3.ltd.ovh/public/cat_z3.ome.zarr/0?anonymous=true");
        System.out.println("testS3V3Public: " + zp);
        Assert.assertEquals(zp.getStorageType(), StorageType.S3);
        Assert.assertEquals(new ComparableVersion("3"), zp.getZarrVersion());
        Assert.assertEquals(new ComparableVersion("0.5"), zp.getNgffVersion());
    }

    // /**
    // * Test private S3 storage, zarr v2.
    // *
    // * @throws IOException If an I/O error occurs.
    // */
    // @Test
    // public void testS3V2Private() throws IOException {
    // ZarrInfo zp = new ZarrInfo("s3://s3.ltd.ovh/private/cat_z2.ome.zarr/0?profile=ltd");
    // System.out.println("testS3V2Private: " + zp);
    // Assert.assertEquals(zp.getStorageType(), StorageType.S3);
    // Assert.assertEquals(new ComparableVersion("2"), zp.getZarrVersion());
    // Assert.assertEquals(new ComparableVersion("0.4"), zp.getNgffVersion());
    // }

    // /**
    // * Test private S3 storage, zarr v3.
    // *
    // * @throws IOException If an I/O error occurs.
    // */
    // @Test
    // public void testS3V3Private() throws IOException {
    // ZarrInfo zp = new ZarrInfo("s3://s3.ltd.ovh/private/cat_z3.ome.zarr/0?profile=ltd");
    // System.out.println("testS3V3Private: " + zp);
    // Assert.assertEquals(zp.getStorageType(), StorageType.S3);
    // Assert.assertEquals(new ComparableVersion("3"), zp.getZarrVersion());
    // Assert.assertEquals(new ComparableVersion("0.5"), zp.getNgffVersion());
    // }

    /**
     * Write a test Zarr file.
     *
     * @param sizeT       Number of time points.
     * @param sizeC       Number of channels.
     * @param sizeZ       Number of z-sections.
     * @param sizeY       Number of rows.
     * @param sizeX       Number of columns.
     * @param pixelType   Pixel type.
     * @param resolutions Number of resolutions.
     * @return Path to the test Zarr file.
     * @throws IOException If an I/O error occurs.
     */
    public Path writeTestZarr(int sizeT, int sizeC, int sizeZ, int sizeY, int sizeX,
        String pixelType, int resolutions) throws IOException {
        Path input = ZarrPixelBufferTest.fake("sizeT", Integer.toString(sizeT), "sizeC",
            Integer.toString(sizeC), "sizeZ", Integer.toString(sizeZ), "sizeY",
            Integer.toString(sizeY), "sizeX", Integer.toString(sizeX), "pixelType", pixelType,
            "resolutions", Integer.toString(resolutions));
        Path output = tmpDir.getRoot().toPath().resolve("output.zarr");
        ZarrPixelBufferTest.assertBioFormats2Raw(input, output);
        return output;
    }
}
