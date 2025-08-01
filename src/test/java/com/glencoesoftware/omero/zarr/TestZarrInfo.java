package com.glencoesoftware.omero.zarr;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.maven.artifact.versioning.ComparableVersion;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.glencoesoftware.omero.zarr.compat.ZarrInfo;

public class TestZarrInfo {
    
    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Test
    public void testZarrInfo() throws IOException {
        String v0_4_local = writeTestZarr(1,1,1,256,256,"uint8",1).toString()+"/0";
        String v0_5_http = "https://uk1s3.embassy.ebi.ac.uk/idr/share/ome2024-ngff-challenge/0.0.5/6001240_labels.zarr";
    
        ZarrInfo zp = new ZarrInfo(v0_4_local);
        Assert.assertFalse(zp.isRemote());
        Assert.assertEquals(new ComparableVersion("2"), zp.getZarrVersion());
        Assert.assertEquals(new ComparableVersion("0.4"), zp.getNgffVersion());

        zp = new ZarrInfo(v0_5_http);
        Assert.assertTrue(zp.isRemote());
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
