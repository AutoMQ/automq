#!/usr/bin/env bash

source=""
target=""

while [ $# -gt 0 ]; do
    case "$1" in
        -s)
            source="$2"
            shift 2
            ;;
        -t)
            target="$2"
            shift 2
            ;;
        *)
            echo "Error: unknown arg Key: $1" >&2
            exit 1
            ;;
    esac
done

if [ -z "$source" ]; then
    echo "Error: source invalid" >&2
    exit 1
fi

if [ -z "$target" ]; then
    echo "Error: target invalid" >&2
    exit 1
fi

echo "[PreStart] ungzip"
for f in $source/*.tgz; do
  tar -xzf "$f" -C "$target" --one-top-level=kafka --strip-components=1 --overwrite
done
echo "[PreStart] chmod"
find "$target" -type f -name "*.sh" -exec chmod a+x {} \;
