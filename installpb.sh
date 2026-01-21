#/bin/bash

OMERO_DIST=/opt/omero/server/OMERO.server
OZPB=https://artifacts.glencoesoftware.com/artifactory/gs-omero-snapshots-local/com/glencoesoftware/omero/omero-zarr-pixel-buffer/0.6.1/omero-zarr-pixel-buffer-0.6.1.jar

# remove older jzarr in case it's there
rm -f $OMERO_DIST/lib/server/jzarr.jar

# Install omero-zarr-pixel-buffer
wget -P $OMERO_DIST/lib/server $OZPB
wget -P $OMERO_DIST/lib/server https://repo.maven.apache.org/maven2/com/github/ben-manes/caffeine/caffeine/3.1.8/caffeine-3.1.8.jar
wget -P $OMERO_DIST/lib/server https://repo.maven.apache.org/maven2/dev/zarr/jzarr/0.4.2/jzarr-0.4.2.jar
wget -P $OMERO_DIST/lib/server https://repo.maven.apache.org/maven2/org/lasersonlab/s3fs/2.2.3/s3fs-2.2.3.jar
wget -P $OMERO_DIST/lib/server https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/1.12.659/aws-java-sdk-s3-1.12.659.jar
wget -P $OMERO_DIST/lib/server https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/1.12.659/aws-java-sdk-core-1.12.659.jar
