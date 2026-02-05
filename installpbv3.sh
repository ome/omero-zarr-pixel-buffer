#/bin/bash

# copy the build jar over first
# scp /Users/dom/Repos/omero-zarr-pixel-buffer/build/libs/omero-zarr-pixel-buffer-0.6.2-SNAPSHOT.jar ansible@omero.home:/tmp/

OMERO_DIST=/opt/omero/server/OMERO.server
OZPB=/tmp/omero-zarr-pixel-buffer-0.6.2-SNAPSHOT.jar

awsSdkVersion=2.34.6
jacksonVersion=2.20.0
logbackVersion=1.3.14
zarrJavaVersion=0.0.10
formatsVersion=8.3.0

# Install omero-zarr-pixel-buffer
mv $OZPB $OMERO_DIST/lib/server

# Remove older jars
cd $OMERO_DIST/lib/server
rm -rf aws-java-sdk-* jackson-annotations.jar jackson-core.jar jackson-databind.jar logback-classic.jar logback-core.jar omero-zarr-pixel-buffer-* s3fs-2.2.3.jar caffeine-3.1.8.jar jzarr-0.4.2.jar

# Get all the dependancies
wget -P $OMERO_DIST/lib/server https://repo1.maven.org/maven2/software/amazon/awssdk/s3/${awsSdkVersion}/s3-${awsSdkVersion}.jar
wget -P $OMERO_DIST/lib/server https://repo1.maven.org/maven2/software/amazon/awssdk/auth/${awsSdkVersion}/auth-${awsSdkVersion}.jar
wget -P $OMERO_DIST/lib/server https://repo1.maven.org/maven2/software/amazon/awssdk/aws-core/${awsSdkVersion}/aws-core-${awsSdkVersion}.jar
wget -P $OMERO_DIST/lib/server https://repo1.maven.org/maven2/software/amazon/awssdk/sdk-core/${awsSdkVersion}/sdk-core-${awsSdkVersion}.jar
wget -P $OMERO_DIST/lib/server https://repo1.maven.org/maven2/software/amazon/awssdk/annotations/${awsSdkVersion}/annotations-${awsSdkVersion}.jar
wget -P $OMERO_DIST/lib/server https://repo1.maven.org/maven2/software/amazon/awssdk/regions/${awsSdkVersion}/regions-${awsSdkVersion}.jar
wget -P $OMERO_DIST/lib/server https://repo1.maven.org/maven2/software/amazon/awssdk/identity-spi/${awsSdkVersion}/identity-spi-${awsSdkVersion}.jar
wget -P $OMERO_DIST/lib/server https://repo1.maven.org/maven2/software/amazon/awssdk/utils/${awsSdkVersion}/utils-${awsSdkVersion}.jar
wget -P $OMERO_DIST/lib/server https://repo1.maven.org/maven2/software/amazon/awssdk/http-client-spi/${awsSdkVersion}/http-client-spi-${awsSdkVersion}.jar
wget -P $OMERO_DIST/lib/server https://repo1.maven.org/maven2/software/amazon/awssdk/retries/${awsSdkVersion}/retries-${awsSdkVersion}.jar
wget -P $OMERO_DIST/lib/server https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/${awsSdkVersion}/url-connection-client-${awsSdkVersion}.jar

wget -P $OMERO_DIST/lib/server https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/${jacksonVersion}/jackson-core-${jacksonVersion}.jar
wget -P $OMERO_DIST/lib/server https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/${jacksonVersion}/jackson-databind-${jacksonVersion}.jar
wget -P $OMERO_DIST/lib/server https://repo1.maven.org/maven2/com/fasterxml/jackson/datatype/jackson-datatype-jdk8/${jacksonVersion}/jackson-datatype-jdk8-${jacksonVersion}.jar
wget -P $OMERO_DIST/lib/server https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.20/jackson-annotations-2.20.jar

wget -P $OMERO_DIST/lib/server https://repo.maven.apache.org/maven2/ch/qos/logback/logback-classic/${logbackVersion}/logback-classic-${logbackVersion}.jar
wget -P $OMERO_DIST/lib/server https://repo.maven.apache.org/maven2/ch/qos/logback/logback-core/${logbackVersion}/logback-core-${logbackVersion}.jar
wget -P $OMERO_DIST/lib/server https://repo.maven.apache.org/maven2/dev/zarr/zarr-java/${zarrJavaVersion}/zarr-java-${zarrJavaVersion}.jar

wget -P $OMERO_DIST/lib/server https://repo.maven.apache.org/maven2/com/github/ben-manes/caffeine/caffeine/3.1.8/caffeine-3.1.8.jar
wget -P $OMERO_DIST/lib/server https://repo.maven.apache.org/maven2/org/apache/tika/tika-core/1.28.5/tika-core-1.28.5.jar
wget -P $OMERO_DIST/lib/server https://repo1.maven.org/maven2/com/squareup/okhttp/okhttp/2.7.5/okhttp-2.7.5.jar
wget -P $OMERO_DIST/lib/server https://repo1.maven.org/maven2/com/github/luben/zstd-jni/1.5.5-7/zstd-jni-1.5.5-7.jar
wget -P $OMERO_DIST/lib/server https://repo1.maven.org/maven2/com/scalableminds/blosc-java/0.1-1.21.4/blosc-java-0.1-1.21.4.jar

chown -R omero-server:omero-server -P $OMERO_DIST/lib/server
