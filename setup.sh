#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPS_DIR="$(mktemp -d)"

cleanup() {
    rm -rf "$DEPS_DIR"
}
trap cleanup EXIT

echo "--- installing jaft"
git clone --quiet git@github.com:Rafee-Mohamed/jaft.git "$DEPS_DIR/jaft"
mvn -f "$DEPS_DIR/jaft/pom.xml" install -DskipTests -q
echo "    installed to ~/.m2/repository"

echo "--- installing versioned-index"
git clone --quiet git@github.com:Rafee-Mohamed/versioned-index.git "$DEPS_DIR/versioned-index"
mvn -f "$DEPS_DIR/versioned-index/pom.xml" install -DskipTests -q
echo "    installed to ~/.m2/repository"

echo "--- building axis"
mvn -f "$SCRIPT_DIR/pom.xml" package -DskipTests -q
echo "    server : $SCRIPT_DIR/server/target/server-0.1.0-SNAPSHOT.jar"
echo "    axisctl: $SCRIPT_DIR/axisctl/target/axisctl-0.1.0-SNAPSHOT.jar"

JAR="$SCRIPT_DIR/axisctl/target/axisctl-0.1.0-SNAPSHOT.jar"

echo ""
echo "setup complete. add this alias to your shell profile:"
echo ""
echo "  alias axisctl='java \\"
echo "    --enable-native-access=ALL-UNNAMED \\"
echo "    --sun-misc-unsafe-memory-access=allow \\"
echo "    -Dio.grpc.netty.shaded.io.netty.noUnsafe=true \\"
echo "    -jar $JAR'"
echo ""
echo "the alias avoids repeating the full java -jar invocation each time."
echo "--enable-native-access is required by JLine's native terminal library."
echo "--sun-misc-unsafe-memory-access=allow suppresses Protobuf Unsafe warnings on Java 25."
echo "the Netty flag prevents a separate Unsafe warning from gRPC's shaded transport."
echo ""
