#!/bin/sh
# 构建 Android 可直接执行的 Go 代理二进制。
#
# 不需要 NDK：Android 内核就是 Linux，纯 Go（CGO_ENABLED=0）产出的
# linux/arm64 静态二进制可以直接在 Android 上跑。
# 原仓库的 build.sh 走 GOOS=android + CGO 是为了编 JNI .so，那条路才需要 NDK。
set -e

OUT=${1:-build}
mkdir -p "$OUT"

# 沙箱/受限环境里 Go 默认缓存目录可能不可写
export GOTMPDIR=${GOTMPDIR:-/tmp/gotmp}
export GOCACHE=${GOCACHE:-/tmp/gocache}
mkdir -p "$GOTMPDIR" "$GOCACHE"

build() {
  arch=$1; goarch=$2
  echo "==> building $arch"
  CGO_ENABLED=0 GOOS=linux GOARCH=$goarch \
    go build -trimpath -ldflags="-w -s" -o "$OUT/goproxy-$arch" .
  ls -l "$OUT/goproxy-$arch" | awk '{print "    " $5 " bytes  " $9}'
}

build arm64-v8a arm64
build armeabi-v7a arm

echo
echo "产物："
ls -1 "$OUT"/goproxy-* 2>/dev/null
echo
echo "用法：推到设备任意可执行目录，chmod +x 后运行，监听 :5575"
echo "  /proxy?url=<链1,链2,...>&thread=12&chunkSize=1024&perUrl=2"
echo "  /health  健康检查"
