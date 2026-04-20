#!/bin/bash

# Target OMERO server lib directory (modify as needed)
OMERO_LIBS=${OMERO_LIBS:-/opt/omero/server/OMERO.server/lib/server}

# Versions
zarrJavaVersion=0.1.1
awsSdkVersion=2.34.6
jacksonVersion=2.20.0
reactiveStreamsVersion=1.0.4
nettyVersion=4.1.108.Final

echo "Installing omero-zarr-pixel-buffer dependencies to: $OMERO_LIBS"

if [ ! -d "$OMERO_LIBS" ]; then
    echo "ERROR: Directory $OMERO_LIBS does not exist"
    exit 1
fi

install_deps() {
    local DEST=$1

    # Remove older installation
    echo "Removing potential older installation from $DEST..."
    rm -f $1/omero-zarr-pixel-buffer*.jar $1/caffeine*.jar $1/jzarr*.jar $1/s3fs*.jar $1/aws-java-*.jar $1/okhttp*.jar $1/okio*.jar

    echo "Downloading dependencies to $DEST..."

    # Core zarr dependencies
    wget -q -P "$DEST" https://repo.maven.apache.org/maven2/com/github/ben-manes/caffeine/caffeine/3.1.8/caffeine-3.1.8.jar
    wget -q -P "$DEST" https://repo.maven.apache.org/maven2/dev/zarr/zarr-java/${zarrJavaVersion}/zarr-java-${zarrJavaVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/com/scalableminds/blosc-java/0.1-1.21.4/blosc-java-0.1-1.21.4.jar

    # Jackson datatype
    wget -q -P "$DEST" https://repo1.maven.org/maven2/com/fasterxml/jackson/datatype/jackson-datatype-jdk8/${jacksonVersion}/jackson-datatype-jdk8-${jacksonVersion}.jar

    # OkHttp and OkIO (updated versions)
    wget -q -P "$DEST" https://repo1.maven.org/maven2/com/squareup/okhttp3/okhttp/4.12.0/okhttp-4.12.0.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/com/squareup/okio/okio-jvm/3.6.0/okio-jvm-3.6.0.jar

    # Kotlin stdlib (required by okhttp)
    wget -q -P "$DEST" https://repo1.maven.org/maven2/org/jetbrains/kotlin/kotlin-stdlib/1.9.10/kotlin-stdlib-1.9.10.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/org/jetbrains/kotlin/kotlin-stdlib-common/1.9.10/kotlin-stdlib-common-1.9.10.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/org/jetbrains/kotlin/kotlin-stdlib-jdk7/1.9.10/kotlin-stdlib-jdk7-1.9.10.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/org/jetbrains/kotlin/kotlin-stdlib-jdk8/1.9.10/kotlin-stdlib-jdk8-1.9.10.jar

    # AWS SDK v2 S3 and core modules
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/s3/${awsSdkVersion}/s3-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/auth/${awsSdkVersion}/auth-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/aws-core/${awsSdkVersion}/aws-core-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/sdk-core/${awsSdkVersion}/sdk-core-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/annotations/${awsSdkVersion}/annotations-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/regions/${awsSdkVersion}/regions-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/identity-spi/${awsSdkVersion}/identity-spi-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/utils/${awsSdkVersion}/utils-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/http-client-spi/${awsSdkVersion}/http-client-spi-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/retries/${awsSdkVersion}/retries-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/${awsSdkVersion}/url-connection-client-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/apache-client/${awsSdkVersion}/apache-client-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/arns/${awsSdkVersion}/arns-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/aws-query-protocol/${awsSdkVersion}/aws-query-protocol-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/aws-xml-protocol/${awsSdkVersion}/aws-xml-protocol-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/checksums/${awsSdkVersion}/checksums-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/checksums-spi/${awsSdkVersion}/checksums-spi-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/crt-core/${awsSdkVersion}/crt-core-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/endpoints-spi/${awsSdkVersion}/endpoints-spi-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/http-auth/${awsSdkVersion}/http-auth-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/http-auth-aws/${awsSdkVersion}/http-auth-aws-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/http-auth-aws-eventstream/${awsSdkVersion}/http-auth-aws-eventstream-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/http-auth-spi/${awsSdkVersion}/http-auth-spi-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/json-utils/${awsSdkVersion}/json-utils-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/metrics-spi/${awsSdkVersion}/metrics-spi-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/netty-nio-client/${awsSdkVersion}/netty-nio-client-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/profiles/${awsSdkVersion}/profiles-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/protocol-core/${awsSdkVersion}/protocol-core-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/retries-spi/${awsSdkVersion}/retries-spi-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/third-party-jackson-core/${awsSdkVersion}/third-party-jackson-core-${awsSdkVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/software/amazon/awssdk/utils-lite/${awsSdkVersion}/utils-lite-${awsSdkVersion}.jar

    # Transitive AWS SDK dependencies
    wget -q -P "$DEST" https://repo1.maven.org/maven2/org/reactivestreams/reactive-streams/${reactiveStreamsVersion}/reactive-streams-${reactiveStreamsVersion}.jar

    # Netty dependencies (required by netty-nio-client)
    wget -q -P "$DEST" https://repo1.maven.org/maven2/io/netty/netty-buffer/${nettyVersion}/netty-buffer-${nettyVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/io/netty/netty-codec/${nettyVersion}/netty-codec-${nettyVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/io/netty/netty-codec-http/${nettyVersion}/netty-codec-http-${nettyVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/io/netty/netty-codec-http2/${nettyVersion}/netty-codec-http2-${nettyVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/io/netty/netty-common/${nettyVersion}/netty-common-${nettyVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/io/netty/netty-handler/${nettyVersion}/netty-handler-${nettyVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/io/netty/netty-resolver/${nettyVersion}/netty-resolver-${nettyVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/io/netty/netty-transport/${nettyVersion}/netty-transport-${nettyVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/io/netty/netty-transport-classes-epoll/${nettyVersion}/netty-transport-classes-epoll-${nettyVersion}.jar
    wget -q -P "$DEST" https://repo1.maven.org/maven2/io/netty/netty-transport-native-unix-common/${nettyVersion}/netty-transport-native-unix-common-${nettyVersion}.jar

    echo "Dependencies downloaded successfully."
}

# Download dependencies
install_deps "$OMERO_LIBS"

echo ""
echo "Dependencies installed to: $OMERO_LIBS"
echo ""
echo "NOTE: You must manually copy the omero-zarr-pixel-buffer JAR to $OMERO_LIBS"
echo ""
echo "Then restart OMERO server."
