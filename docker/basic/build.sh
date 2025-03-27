#!/bin/bash

dir=$(dirname $0)
tag=$(date +%Y.%m.%d)
dockerfile="$dir/Dockerfile"
# huawei: use mirrors.huaweicloud.com
mirror=""

while [ $# -gt 0 ]; do
    case "$1" in
        -tag)
            tag="$2"
            shift 2
            ;;
        -df)
            dockerfile="$2"
            shift 2
            ;;
        -m)
            mirror="$2"
            shift 2
            ;;
        *)
            echo "Error: unknown arg Key: $1" >&2
            exit 1
            ;;
    esac
done

echo "build work dir: $dir"
echo "build from $dockerfile"
echo "build tag: $tag"

mirror_domain=""
if [ ! -z "$mirror" ]; then
    echo "debian source: $mirror"
    if [ "$mirror" = "huawei" ]; then
        mirror_domain="mirrors.huaweicloud.com"
    else
        echo "Error: debian source invalid.">&2
        echo "  huawei: mirrors.huaweicloud.com">&2
        exit 1
    fi
fi

if [ -z "$tag" ]; then
    echo "Error: tag value invalid" >&2
    exit 1
fi

if [ -z "$dockerfile" ]; then
    echo "Error: Dockerfile path invalid" >&2
    exit 1
fi
if [ ! -f "$dockerfile" ]; then
    echo "Error: Dockerfile file not exists" >&2
    exit 1
fi

echo "start build image."
cmd="docker build -t automq-basic:$tag -f $dockerfile $dir --build-arg DEBIAN_MIRROR=$mirror_domain"
# Uncomment to debug the build command
#echo "$cmd"
eval $cmd
echo "finish build image."
