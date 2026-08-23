// 代理核心逻辑：
// 1. 解析客户端 Range 请求
// 2. 先下载首块，确认总大小并回写响应头
// 3. 按块并发拉取剩余数据
// 4. 按顺序写回客户端，尽量保持流式输出
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"time"
)

type Player struct {
	client    *http.Client
	header    http.Header
	start     int64
	end       int64
	thread    int
	chunkSize int64
	url       string
	// pool 为多条等价下载链的轮转池。
	// 光鸭这类按签名链限速的源，必须多链并行才能叠加吞吐。
	pool *URLPool
}

// NewPlayer 根据上游请求头和代理参数创建一个下载器实例。
// 这里只透传和目标源站关系最强的几个请求头，避免把无关头信息带过去。
//
// url 支持逗号分隔的多条等价链；perURL 限制单条链的并发数（<=0 表示不限）。
func NewPlayer(header http.Header, thread, chunkSizeKB int, url string, perURL int) *Player {
	h := http.Header{}
	for _, key := range []string{"User-Agent", "Cookie", "Referer"} {
		if v := header.Get(key); v != "" {
			h.Set(key, v)
		}
	}
	start, end := parseRange(header.Get("Range"))

	urls := ParseURLs(url)
	if len(urls) == 0 {
		urls = []string{url}
	}

	return &Player{
		client: &http.Client{
			// 不设置整体超时，避免长视频或慢速源站被客户端统一截断。
			Timeout: 0,
			Transport: &http.Transport{
				// 某些视频源证书配置不规范，这里保持兼容性优先。
				TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
				MaxIdleConns:          100,
				MaxIdleConnsPerHost:   20,
				IdleConnTimeout:       60 * time.Second,
				ResponseHeaderTimeout: 30 * time.Second,
				DisableKeepAlives:     false,
				DialContext: (&net.Dialer{
					Timeout:   10 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
			},
		},
		header:    h,
		start:     start,
		end:       end,
		thread:    thread,
		chunkSize: int64(chunkSizeKB) * 1024,
		url:       urls[0],
		pool:      NewURLPool(urls, perURL),
	}
}

// Play 执行一次完整的代理传输。
// 它会先发送首块和响应头，再并发获取剩余数据块并按顺序写回。
func (p *Player) Play(w http.ResponseWriter, ctx context.Context) error {
	// 先下载首块，用于确认文件总大小并立即开始回传响应。
	s, e, err := p.downloadFirst(w, ctx)
	if err != nil {
		return err
	}
	fileSize := e + 1
	log.Printf("文件大小: %d MB, 线程: %d, 块大小: %d KB",
		fileSize/1024/1024, p.thread, p.chunkSize/1024)

	flusher, _ := w.(http.Flusher)
	if flusher != nil {
		flusher.Flush()
	}

	results := make([][]byte, p.thread)
	var wg sync.WaitGroup

	for start := s; start < fileSize; start += int64(p.chunkSize) * int64(p.thread) {
		// 上层如果主动断开连接，这里尽快停止后续下载。
		select {
		case <-ctx.Done():
			log.Printf("请求被取消")
			return ctx.Err()
		default:
		}

		activeThreads := 0
		// 每一轮批量并发下载都记录各自的错误，便于统一判断是否中断传输。
		chunkErrors := make([]error, p.thread)

		for i := 0; i < p.thread; i++ {
			chunkStart := start + int64(i)*p.chunkSize
			chunkEnd := chunkStart + p.chunkSize
			if chunkStart >= fileSize {
				break
			}
			if chunkEnd > fileSize {
				chunkEnd = fileSize
			}

			results[i] = nil
			chunkErrors[i] = nil
			activeThreads++
			wg.Add(1)

			go func(idx int, cs, ce int64) {
				defer wg.Done()

				// 单个分块设置独立超时，避免某一块永久卡住拖死整轮任务。
				downloadCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
				defer cancel()

				// 每个分块独立重试，提高弱网或不稳定源站下的成功率。
				var data []byte
				var err error
				for retry := 0; retry < 3; retry++ {
					data, _, _, err = p.downloadChunk(downloadCtx, cs, ce, 3)
					if err == nil {
						break
					}
					log.Printf("块 %d-%d 第%d次重试", cs, ce-1, retry+1)
					time.Sleep(time.Second * time.Duration(retry+1))
				}

				if err != nil {
					log.Printf("⚠️ 块 %d-%d 下载彻底失败: %v", cs, ce-1, err)
					// 记录失败块，后面由主协程统一决定是否终止本次传输。
					chunkErrors[idx] = fmt.Errorf("数据块 %d (%d-%d) 下载失败: %w", idx, cs, ce-1, err)
					return
				}
				results[idx] = data
			}(i, chunkStart, chunkEnd)
		}

		// 当前这批块必须全部结束后，才能按顺序向客户端写出。
		wg.Wait()

		// 只要某块彻底失败，就直接中止，避免客户端拿到拼接不完整的数据流。
		for i := 0; i < activeThreads; i++ {
			if chunkErrors[i] != nil {
				log.Printf("❌ %v", chunkErrors[i])
				return chunkErrors[i] // 中止传输，不返回异常数据流
			}
		}

		// 等待批量任务结束后再次确认调用方是否已经取消。
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 下载顺序可以并发，写出顺序必须稳定，否则客户端数据会错位。
		for i := 0; i < activeThreads; i++ {
			_, err = w.Write(results[i])
			if err != nil {
				log.Printf("写入失败: %v", err)
				return err
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}

	log.Printf("下载完成")
	return nil
}

// downloadFirst 下载首块数据，并根据源站返回的 Content-Range 确定完整文件大小。
// 这个阶段还负责把响应头回写给客户端。
func (p *Player) downloadFirst(w http.ResponseWriter, ctx context.Context) (int64, int64, error) {
	start, end := p.start, p.end
	if end <= 0 {
		// 客户端没有给出明确结束位置时，先试探取一个较小首块。
		end = 100
	} else {
		// Range 结束位是闭区间，这里转成内部处理更方便的开区间。
		end += 1
	}
	end = start + min(end, p.chunkSize)

	chunk, header, status, err := p.downloadChunk(ctx, start, end, 3)
	if err != nil {
		return 0, 0, err
	}

	matches := crRegex.FindStringSubmatch(header.Get("Content-Range"))
	if len(matches) != 4 {
		return 0, 0, errors.New("未获取到文件总大小")
	}
	totalLength, _ := strconv.ParseInt(matches[3], 10, 64)

	if p.end <= 0 {
		// 没指定结束位置时，默认拉取到源文件尾部。
		end = totalLength - 1
	} else {
		end = p.end
	}

	h := w.Header()
	h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, totalLength))

	// 只透传对播放有意义的头。
	// 关键：绝不能把源站的 Connection / Content-Length / Transfer-Encoding 抄过来。
	// 光鸭 CDN 每个分块响应都带 Connection: close，抄过去会让播放器在
	// 收完第一块（约 1MB）后就断开，于是只拿到文件开头一小段，
	// ExoPlayer 报 ERROR_CODE_PARSING_CONTAINER_UNSUPPORTED（容器无法解析）。
	// 这类逐跳头（hop-by-hop）本来就不该被代理转发。
	skip := map[string]bool{
		"Content-Range":       true,
		"Content-Length":      true,
		"Connection":          true,
		"Transfer-Encoding":   true,
		"Keep-Alive":          true,
		"Proxy-Connection":    true,
		"Trailer":             true,
		"Upgrade":             true,
		"Te":                  true,
		"Content-Disposition": true, // 带 attachment 会让某些播放器当下载处理
	}
	for k, v := range header {
		if !skip[http.CanonicalHeaderKey(k)] {
			h[k] = v
		}
	}
	// 明确告知总长与可 Range，播放器据此做 seek
	h.Set("Accept-Ranges", "bytes")
	h.Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	w.WriteHeader(status)

	_, err = w.Write(chunk)
	if err != nil {
		return 0, 0, err
	}

	return start + int64(len(chunk)), end, nil
}

// downloadChunk 负责下载一个指定字节区间。
// 返回值包括数据内容、响应头和状态码，方便上层首块逻辑复用。
func (p *Player) downloadChunk(ctx context.Context, start, end int64, maxRetries int) ([]byte, http.Header, int, error) {
	var lastErr error
	for retry := 0; retry < maxRetries; retry++ {
		// 每次尝试都从池里换一条链：既能叠加吞吐，也让失败的链自动被绕过。
		idx, target := p.pool.Acquire()
		if target == "" {
			target = p.url
		}
		data, header, status, err := p.doChunk(ctx, target, start, end)
		p.pool.Release(idx)
		if err == nil {
			return data, header, status, nil
		}
		lastErr = err
		if retry < maxRetries-1 {
			select {
			case <-time.After(time.Duration(retry+1) * 400 * time.Millisecond):
			case <-ctx.Done():
				return nil, nil, -1, ctx.Err()
			}
		}
	}
	return nil, nil, -1, fmt.Errorf("重试%d次失败: %v", maxRetries, lastErr)
}

// doChunk 向指定链发一次 Range 请求。
func (p *Player) doChunk(ctx context.Context, target string, start, end int64) ([]byte, http.Header, int, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", target, nil)
	if err != nil {
		return nil, nil, -1, err
	}
	req.Header = p.header.Clone()
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end-1))

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, nil, -1, err
	}
	defer resp.Body.Close()

	// 某些源站即使带了 Range 也会直接返回 200，这里一并兼容。
	if resp.StatusCode == 206 || resp.StatusCode == 200 {
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, nil, -1, err
		}
		return data, resp.Header, resp.StatusCode, nil
	}

	return nil, nil, -1, fmt.Errorf("状态码: %d", resp.StatusCode)
}

var crRegex = regexp.MustCompile(`bytes\s+(\d+)-(\d+)/(\d+)`)
var seRegex = regexp.MustCompile(`bytes=(\d+)-(\d*)`)

// parseRange 解析客户端传入的 Range 请求头。
// 当请求头不存在或格式不匹配时，返回从 0 到文件末尾的默认语义。
func parseRange(rangeStr string) (int64, int64) {
	match := seRegex.FindStringSubmatch(rangeStr)
	if len(match) == 0 {
		return 0, -1
	}
	start, _ := strconv.ParseInt(match[1], 10, 64)
	end := int64(-1)
	if match[2] != "" {
		end, _ = strconv.ParseInt(match[2], 10, 64)
	}
	return start, end
}
