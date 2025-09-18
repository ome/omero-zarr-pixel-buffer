package com.glencoesoftware.omero.zarr;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.maven.artifact.versioning.ComparableVersion;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.glencoesoftware.omero.zarr.compat.ZarrInfo;
import com.glencoesoftware.omero.zarr.compat.ZarrInfo.StorageType;

public class TestZarrInfo {
    
    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Test
    public void testLocalV2N04() throws IOException {
        String v0_4_local = writeTestZarr(1,1,1,256,256,"uint8",1).toString()+"/0";
    
        ZarrInfo zp = new ZarrInfo(v0_4_local);
        Assert.assertEquals(zp.getStorageType(), StorageType.FILE);
        Assert.assertEquals(new ComparableVersion("2"), zp.getZarrVersion());
        Assert.assertEquals(new ComparableVersion("0.4"), zp.getNgffVersion());
    }

    @Test
    public void testHTTPV2N04() throws IOException {
        ZarrInfo zp = new ZarrInfo("https://uk1s3.embassy.ebi.ac.uk/idr/zarr/v0.4/idr0101A/13457227.zarr");
        Assert.assertEquals(zp.getStorageType(), StorageType.HTTP);
        Assert.assertEquals(new ComparableVersion("2"), zp.getZarrVersion());
        Assert.assertEquals(new ComparableVersion("0.4"), zp.getNgffVersion());
    }

    @Test
    public void testS3V2N04() throws IOException {
        ZarrInfo zp = new ZarrInfo("s3://s3.us-east-1.amazonaws.com/gs-public-zarr-archive/CMU-1.ome.zarr/0?anonymous=true");
        Assert.assertEquals(zp.getStorageType(), StorageType.S3);
        Assert.assertEquals(new ComparableVersion("2"), zp.getZarrVersion());
        Assert.assertEquals(new ComparableVersion("0.4"), zp.getNgffVersion());
    }

    @Test
    public void testHTTPV3N05() throws IOException {
        ZarrInfo zp = new ZarrInfo("https://uk1s3.embassy.ebi.ac.uk/idr/share/ome2024-ngff-challenge/0.0.5/6001240_labels.zarr");
        Assert.assertEquals(zp.getStorageType(), StorageType.HTTP);
        Assert.assertEquals(new ComparableVersion("3"), zp.getZarrVersion());
        Assert.assertEquals(new ComparableVersion("0.5"), zp.getNgffVersion());
    }

    public Path writeTestZarr(
            int sizeT,
            int sizeC,
            int sizeZ,
            int sizeY,
            int sizeX,
            String pixelType,
            int resolutions) throws IOException {
        Path input = ZarrPixelBufferTest.fake(
                "sizeT", Integer.toString(sizeT),
                "sizeC", Integer.toString(sizeC),
                "sizeZ", Integer.toString(sizeZ),
                "sizeY", Integer.toString(sizeY),
                "sizeX", Integer.toString(sizeX),
                "pixelType", pixelType,
                "resolutions", Integer.toString(resolutions));
        Path output = tmpDir.getRoot().toPath().resolve("output.zarr");
        ZarrPixelBufferTest.assertBioFormats2Raw(input, output);
        return output;
    }
}
