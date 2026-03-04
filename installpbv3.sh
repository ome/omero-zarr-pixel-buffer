#/bin/bash

OMERO_DIST=/opt/omero/server/OMERO.server
OZPB=

jacksonVersion=2.20.0
awsSdkVersion=2.34.6
zarrJavaVersion=0.0.10

# Remove older installation
rm -f $OMERO_DIST/lib/server/omero-zarr-pixel-buffer*.jar $OMERO_DIST/lib/server/caffeine*.jar $OMERO_DIST/lib/server/jzarr*.jar $OMERO_DIST/lib/server/s3fs*.jar $OMERO_DIST/lib/server/aws-java-*.jar

# Install omero-zarr-pixel-buffer
if [ -z "${OZPB:-}" ]; then
    echo "OZPB is not set; you have to manually copy the omero-zarr-pixel-buffer jar to $OMERO_DIST/lib/server"
else
    wget -P $OMERO_DIST/lib/server $OZPB
fi

# Install dependencies
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

wget -P $OMERO_DIST/lib/server https://repo.maven.apache.org/maven2/com/github/ben-manes/caffeine/caffeine/3.1.8/caffeine-3.1.8.jar

wget -P $OMERO_DIST/lib/server https://repo.maven.apache.org/maven2/dev/zarr/zarr-java/${zarrJavaVersion}/zarr-java-${zarrJavaVersion}.jar
wget -P $OMERO_DIST/lib/server https://repo1.maven.org/maven2/com/scalableminds/blosc-java/0.1-1.21.4/blosc-java-0.1-1.21.4.jar
wget -P $OMERO_DIST/lib/server https://repo1.maven.org/maven2/com/squareup/okhttp/okhttp/2.7.5/okhttp-2.7.5.jar

wget -P $OMERO_DIST/lib/server https://repo1.maven.org/maven2/com/fasterxml/jackson/datatype/jackson-datatype-jdk8/${jacksonVersion}/jackson-datatype-jdk8-${jacksonVersion}.jar
