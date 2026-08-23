# goproxy（多链版）

基于 [Silent1566/GoProxyAndroid](https://github.com/Silent1566/GoProxyAndroid)（MIT）改造。

## 改了什么

原版 `/proxy` 只接受**一条** url，在其上开多线程分块下载。
但光鸭网盘是**按签名链限速**的：同一条链上堆并发会被卡速并触发 503。

实测（同一文件、同一时段、各 2 轮取中位数）：

| 模式 | 吞吐 |
|---|---|
| 单条链 + 12 线程（原版） | 0.46 MB/s |
| 6 条链 + 单链限 2 并发（改造后） | 0.62 MB/s |

新增 `pool.go`：多链轮转池，`Acquire()` 把「挑链」和「占位」放在同一把锁内原子完成。
分两步会让多个协程同时选中同一条链、一起冲过上限，退化成单链多并发的慢速模式。

## 接口

```
GET /proxy?url=<链1,链2,...>&thread=12&chunkSize=1024&perUrl=2
GET /health
GET /
```

- `url` 逗号分隔多条等价下载链；只给一条时行为与原版一致
- `perUrl` 单条链最大并发，`<=0` 表示不限，默认 2
- 透传 `Range`、`User-Agent`、`Cookie`、`Referer`

监听 `:5575`。

## 构建

不需要 Android NDK。Android 内核就是 Linux，`CGO_ENABLED=0` 产出的
`linux/arm64` 静态二进制可直接在 Android 上执行：

```bash
sh build_android.sh
# -> build/goproxy-arm64-v8a
# -> build/goproxy-armeabi-v7a
```

原仓库 `build.sh` 走 `GOOS=android` + CGO 是为了编 JNI `.so`，那条路才需要 NDK。

## 在 Android 上运行

**注意**：Android 10 (API 29) 起，SELinux 禁止 app 执行自己私有目录里可写的文件，
所以 app 内「下载二进制 → chmod +x → 启动」会被拒绝。

可用方式：

```bash
# Termux
cp goproxy-arm64-v8a ~/goproxy && chmod +x ~/goproxy && ~/goproxy

# adb
adb push goproxy-arm64-v8a /data/local/tmp/goproxy
adb shell chmod +x /data/local/tmp/goproxy
adb shell /data/local/tmp/goproxy
```

起来后 spider 会自动探测到 `127.0.0.1:5575` 并使用；探测不到则回落内置 Java 代理。
