#/bin/bash

# OMERO server installation directory
OMERO_DIST=/opt/omero/server/OMERO.server
# OMERO zarr pixel buffer jar URL (optional, can be set manually)
OZPB=https://artifacts.glencoesoftware.com/artifactory/gs-omero-snapshots-local/com/glencoesoftware/omero/omero-zarr-pixel-buffer/0.6.1/omero-zarr-pixel-buffer-0.6.1.jar

# remove older jzarr in case it's there
rm -f $OMERO_DIST/lib/server/jzarr.jar

# Install dependencies
wget -P $OMERO_DIST/lib/server https://repo.maven.apache.org/maven2/com/github/ben-manes/caffeine/caffeine/3.1.8/caffeine-3.1.8.jar
wget -P $OMERO_DIST/lib/server https://repo.maven.apache.org/maven2/dev/zarr/jzarr/0.4.2/jzarr-0.4.2.jar
wget -P $OMERO_DIST/lib/server https://repo.maven.apache.org/maven2/org/lasersonlab/s3fs/2.2.3/s3fs-2.2.3.jar
wget -P $OMERO_DIST/lib/server https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/1.12.659/aws-java-sdk-s3-1.12.659.jar
wget -P $OMERO_DIST/lib/server https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/1.12.659/aws-java-sdk-core-1.12.659.jar


# Install omero-zarr-pixel-buffer
if [ -z "${OZPB:-}" ]; then
    echo "OZPB is not set; you have to manually copy the omero-zarr-pixel-buffer jar to $OMERO_DIST/lib/server"
else
    wget -P $OMERO_DIST/lib/server $OZPB
fi
