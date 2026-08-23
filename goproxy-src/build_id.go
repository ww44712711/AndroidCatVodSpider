package main

// BuildID 标识这份二进制的版本。
//
// 为什么需要它：Java 侧启动 goproxy 后只持有一个 Process 引用，
// app 一重启这个引用就丢了，端口上的进程变成孤儿进程 —— 杀不掉，
// 也无法通过比对文件知道它的版本（文件早就换成新的了，跑着的还是旧的）。
// 所以让二进制通过 /health 自报 build，Java 侧比对不一致就调 /quit 让它退出。
//
// 每次改动 Go 代码都要更新这个值。
const BuildID = "2026082309-dns"
