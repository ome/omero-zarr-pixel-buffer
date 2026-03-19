#/bin/bash

# OMERO server lib directory
OMERO_LIBS=/opt/omero/server/OMERO.server/lib/server

# Microservice lib directory
MS_LIBS=/opt/omero/microservice/omero-ms-image-region/lib

# OMERO zarr pixel buffer jar URL (optional, can be set manually)
OZPB=https://merge-ci.openmicroscopy.org/jenkins/job/OMERO-zarr-pixel-buffer-build/78/artifact/omero-zarr-pixel-buffer/build/libs/omero-zarr-pixel-buffer-0.6.2-SNAPSHOT.jar

jacksonVersion=2.20.0
awsSdkVersion=2.34.6
reactiveStreamsVersion=1.0.4 # used by aws
zarrJavaVersion=0.1.0

install () {
    # Remove older installation
    rm -f $1/omero-zarr-pixel-buffer*.jar $1/caffeine*.jar $1/jzarr*.jar $1/s3fs*.jar $1/aws-java-*.jar $1/okhttp*.jar $1/okio*.jar
    # Install dependencies
    wget -P $1 https://repo.maven.apache.org/maven2/com/github/ben-manes/caffeine/caffeine/3.1.8/caffeine-3.1.8.jar
    wget -P $1 https://repo.maven.apache.org/maven2/dev/zarr/zarr-java/${zarrJavaVersion}/zarr-java-${zarrJavaVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/com/scalableminds/blosc-java/0.1-1.21.4/blosc-java-0.1-1.21.4.jar
    wget -P $1 https://repo1.maven.org/maven2/com/squareup/okhttp3/okhttp/4.12.0/okhttp-4.12.0.jar
    wget -P $1 https://repo1.maven.org/maven2/com/squareup/okio/okio-jvm/3.6.0/okio-jvm-3.6.0.jar
    wget -P $1 https://repo1.maven.org/maven2/org/jetbrains/kotlin/kotlin-stdlib/1.9.10/kotlin-stdlib-1.9.10.jar
    wget -P $1 https://repo1.maven.org/maven2/com/fasterxml/jackson/datatype/jackson-datatype-jdk8/${jacksonVersion}/jackson-datatype-jdk8-${jacksonVersion}.jar

    # Additional AWS SDK v2 runtime dependencies
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/s3/${awsSdkVersion}/s3-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/auth/${awsSdkVersion}/auth-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/aws-core/${awsSdkVersion}/aws-core-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/sdk-core/${awsSdkVersion}/sdk-core-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/annotations/${awsSdkVersion}/annotations-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/regions/${awsSdkVersion}/regions-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/identity-spi/${awsSdkVersion}/identity-spi-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/utils/${awsSdkVersion}/utils-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/http-client-spi/${awsSdkVersion}/http-client-spi-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/retries/${awsSdkVersion}/retries-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/${awsSdkVersion}/url-connection-client-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/apache-client/${awsSdkVersion}/apache-client-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/arns/${awsSdkVersion}/arns-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/aws-query-protocol/${awsSdkVersion}/aws-query-protocol-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/aws-xml-protocol/${awsSdkVersion}/aws-xml-protocol-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/checksums/${awsSdkVersion}/checksums-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/checksums-spi/${awsSdkVersion}/checksums-spi-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/crt-core/${awsSdkVersion}/crt-core-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/endpoints-spi/${awsSdkVersion}/endpoints-spi-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/http-auth/${awsSdkVersion}/http-auth-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/http-auth-aws/${awsSdkVersion}/http-auth-aws-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/http-auth-aws-eventstream/${awsSdkVersion}/http-auth-aws-eventstream-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/http-auth-spi/${awsSdkVersion}/http-auth-spi-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/json-utils/${awsSdkVersion}/json-utils-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/metrics-spi/${awsSdkVersion}/metrics-spi-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/netty-nio-client/${awsSdkVersion}/netty-nio-client-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/profiles/${awsSdkVersion}/profiles-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/protocol-core/${awsSdkVersion}/protocol-core-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/retries-spi/${awsSdkVersion}/retries-spi-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/third-party-jackson-core/${awsSdkVersion}/third-party-jackson-core-${awsSdkVersion}.jar
    wget -P $1 https://repo1.maven.org/maven2/software/amazon/awssdk/utils-lite/${awsSdkVersion}/utils-lite-${awsSdkVersion}.jar

    # Transitive AWS SDK v2 dependencies
    wget -P $1 https://repo1.maven.org/maven2/org/reactivestreams/reactive-streams/${reactiveStreamsVersion}/reactive-streams-${reactiveStreamsVersion}.jar

    # Install omero-zarr-pixel-buffer
    if [ -z "${OZPB:-}" ]; then
        echo "OZPB is not set; you have to manually copy the omero-zarr-pixel-buffer jar to $1"
    else
        wget -P $1 $OZPB
    fi
}

if [ -d "$OMERO_LIBS" ]; then
  install $OMERO_LIBS
  echo "Restarting omero-server..."
  systemctl restart omero-server
fi

if [ -d "$MS_LIBS" ]; then
  install $MS_LIBS
  echo "Restarting omero-ms-image-region..."
  systemctl restart omero-ms-image-region
fi
