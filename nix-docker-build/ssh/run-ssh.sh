#/bin/sh

replace() {
    sed -e "s|@BASE_DIR@|$RESOLVE|g" \
        -e "s|@NIX_STORE_CMD@|$NIX_STORE_CMD|g" \
        -e "s|@DOCKER@|$DOCKER|g" \
        "$DIR/$1" \
        > "$DIR/resolved/$1"
}

DIR="$(dirname "$0")"
export RESOLVE="$(cd "$DIR/../.."; pwd)"
bash -c 'cd "$RESOLVE/nix-docker-build"; cargo build'
echo "Resolved: $RESOLVE"
mkdir -p "$DIR/resolved"

DOCKER="$(command -v docker)"
echo "Docker: $DOCKER"
NIX_STORE_CMD="$(command -v nix-store)"
echo "nix-store: $NIX_STORE_CMD"
SSHD="$(command -v sshd)"
echo "SSHD: $SSHD"

replace authorized_keys
replace ssh_config
replace sshd_config

docker build -t griff/nix-static "$DIR"
echo "Running: "$SSHD" -D -f "$DIR/resolved/sshd_config""
"$SSHD" -D -f "$DIR/resolved/sshd_config"