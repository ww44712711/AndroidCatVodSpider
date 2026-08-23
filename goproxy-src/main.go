// 独立运行模式下的入口。
// 这个文件用于直接启动一个本地 HTTP 代理服务，便于单独调试 Go 代理逻辑。
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	// 根路径用于最简单的存活探测。
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	})

	// /proxy 是核心代理入口，必须携带线程数、分块大小和目标地址。
	http.HandleFunc("/proxy", func(w http.ResponseWriter, r *http.Request) {
		params := r.URL.Query()
		thread, chunkSize, url := params.Get("thread"), params.Get("chunkSize"), params.Get("url")

		if thread == "" || chunkSize == "" || url == "" {
			http.Error(w, "参数不完整", http.StatusBadRequest)
			return
		}

		t, err := strconv.Atoi(thread)
		if err != nil {
			http.Error(w, "thread必须为整数", http.StatusBadRequest)
			return
		}
		c, err := strconv.Atoi(chunkSize)
		if err != nil {
			http.Error(w, "chunkSize必须为整数", http.StatusBadRequest)
			return
		}

		// perUrl 限制单条链的并发数。光鸭这类按签名链限速的源必须压住，
		// 否则同一条链上堆并发会被卡速并触发 503。默认 2 是实测较优值。
		perURL := 2
		if v := params.Get("perUrl"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				perURL = n
			}
		}

		player := NewPlayer(r.Header, t, c, url, perURL)
		if player.pool.Size() == 0 {
			http.Error(w, "url 参数无有效链接", http.StatusBadRequest)
			return
		}
		// 不在这里 defer Close()：Play() 返回时可能还有在途下载协程，
		// 提前关闭会让它们拿不到链而失败。池随请求一起被 GC 回收即可。

		// 这里不做 panic 恢复，尽量把错误按正常流程返回并记录日志。
		if err := player.Play(w, r.Context()); err != nil {
			log.Printf("播放错误: %v", err)
			// 一旦已经开始写响应体，就不要再额外写错误响应，避免破坏数据流。
		}
	})

	// 健康检查接口，方便上层轮询代理是否正常可用。
	// build 字段让 Java 侧能判断「5575 上跑的是不是最新版」——
	// app 重启后原来的 Process 引用就丢了，那个进程变成孤儿进程，
	// 光靠比对文件无法知道它的版本，必须让它自报。
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status": "healthy", "type": "go", "port": %d, "build": "%s", "timestamp": "%s"}`,
			5575, BuildID, time.Now().Format(time.RFC3339))
	})

	// 让上层可以主动要求老进程退出，解决孤儿进程占端口的问题。
	http.HandleFunc("/quit", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "bye")
		go func() {
			time.Sleep(200 * time.Millisecond)
			os.Exit(0)
		}()
	})

	log.SetOutput(os.Stdout)
	log.Printf("服务器启动在 :5575")
	log.Fatal(http.ListenAndServe(":5575", nil))
}
