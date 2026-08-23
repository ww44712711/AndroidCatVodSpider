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
	"strings"
	"sync"
	"sync/atomic"
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
	// e 是本次响应窗口的结束位（不是整个文件的结尾），
	// 所以这里是「本窗口的上界」，循环到它为止即可正常结束响应。
	windowEnd := e + 1
	log.Printf("本次窗口: %d-%d (%d MB), 线程: %d, 块大小: %d KB",
		s, e, (windowEnd-s)/1024/1024, p.thread, p.chunkSize/1024)

	flusher, _ := w.(http.Flusher)
	if flusher != nil {
		flusher.Flush()
	}

	if s >= windowEnd {
		return nil
	}

	// ============ 流式写出 ============
	//
	// 原实现是「批量」：一轮起 thread 个分块，wg.Wait() 等**全部**完成后才按序写出。
	// 一轮就是 thread × chunkSize（8×1MB=8MB），光鸭限速下要等很久才凑齐一轮，
	// 首字节延迟实测 7~9 秒，ExoPlayer 早已超时判失败 —— 表现就是「一点速度都没有」。
	//
	// 改为流水线：worker 持续并发预取后面的分块，writer 只要「下一个该写的」就绪
	// 就立刻写出并 Flush，不等同批其它分块。这样首字节 ≈ 单块耗时，
	// 且慢块只阻塞它自己那个位置，不会拖住整批。
	// 内置 Java 代理就是这个设计，实测能稳定播放。

	// 切片。
	//
	// 关键：writer 严格按序，必须等 idx=0 就绪才能写第一个字节。
	// 若首块就用满 chunkSize(1MB)，光鸭单链限速下要 4~5 秒才下完，
	// 这段时间播放器一个字节都拿不到 —— 实测头 5.2 秒只交付 0.25MB，
	// 播放器 3.4 秒就判超时失败。
	// 所以开头用递增的小分片（64KB 起，逐步翻倍到 chunkSize）：
	// 第一片几百毫秒就能就绪，让数据尽快流动起来；
	// 后面再用大分片保证吞吐。
	type piece struct {
		start, end int64 // [start, end)
	}
	var pieces []piece
	ramp := int64(64 * 1024)
	for off := s; off < windowEnd; {
		size := ramp
		if size > p.chunkSize {
			size = p.chunkSize
		}
		end := off + size
		if end > windowEnd {
			end = windowEnd
		}
		pieces = append(pieces, piece{off, end})
		off = end
		if ramp < p.chunkSize {
			ramp *= 2
		}
	}

	total := len(pieces)
	// 每个分块一个就绪信号 + 数据槽
	results := make([][]byte, total)
	errs := make([]error, total)
	done := make([]chan struct{}, total)
	for i := range done {
		done[i] = make(chan struct{})
	}

	// 预取窗口：最多领先 writer 这么多块，避免把整个窗口全读进内存。
	// thread 个在途 + 一点缓冲，内存占用约 (thread+4)×chunkSize。
	lead := p.thread + 4

	fetchCtx, cancelFetch := context.WithCancel(ctx)
	defer cancelFetch()

	// 写到哪一块了，worker 据此控制预取距离
	var writeCursor int64
	var next int64 // 下一个待领取的分块索引

	var wg sync.WaitGroup
	for i := 0; i < p.thread; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				idx := int(atomic.AddInt64(&next, 1) - 1)
				if idx >= total {
					return
				}
				// 别跑太远，等 writer 追上来再继续
				for atomic.LoadInt64(&writeCursor)+int64(lead) < int64(idx) {
					select {
					case <-fetchCtx.Done():
						close(done[idx])
						return
					case <-time.After(30 * time.Millisecond):
					}
				}
				select {
				case <-fetchCtx.Done():
					close(done[idx])
					return
				default:
				}

				pc := pieces[idx]
				var data []byte
				var err error
				// 每块独立超时，避免某块永久卡住
				for retry := 0; retry < 3; retry++ {
					dlCtx, cancel := context.WithTimeout(fetchCtx, 60*time.Second)
					data, _, _, err = p.downloadChunk(dlCtx, pc.start, pc.end, 3)
					cancel()
					if err == nil {
						break
					}
					if fetchCtx.Err() != nil {
						break
					}
					log.Printf("块 %d-%d 第%d次重试", pc.start, pc.end-1, retry+1)
					select {
					case <-time.After(time.Duration(retry+1) * 500 * time.Millisecond):
					case <-fetchCtx.Done():
					}
				}
				if err != nil {
					errs[idx] = fmt.Errorf("数据块 %d (%d-%d) 下载失败: %w", idx, pc.start, pc.end-1, err)
				} else {
					results[idx] = data
				}
				close(done[idx])
			}
		}()
	}

	// writer：严格按顺序，谁就绪就立刻发出去
	var written int64
	for i := 0; i < total; i++ {
		select {
		case <-done[i]:
		case <-ctx.Done():
			cancelFetch()
			log.Printf("请求被取消，已发送 %d 字节", written)
			return ctx.Err()
		}

		if errs[i] != nil {
			cancelFetch()
			log.Printf("❌ %v", errs[i])
			return errs[i]
		}
		data := results[i]
		if data == nil {
			// 被取消时槽位可能为空
			cancelFetch()
			return fmt.Errorf("数据块 %d 缺失", i)
		}

		n, werr := w.Write(data)
		written += int64(n)
		// 及时释放，避免整窗数据堆在内存里
		results[i] = nil
		if werr != nil {
			cancelFetch()
			log.Printf("写入失败: %v", werr)
			return werr
		}
		if flusher != nil {
			flusher.Flush()
		}
		atomic.StoreInt64(&writeCursor, int64(i))
	}

	cancelFetch()
	wg.Wait()
	log.Printf("下载完成，发送 %d 字节", written)
	return nil
}

// downloadFirst 下载首块数据，并根据源站返回的 Content-Range 确定完整文件大小。
// 这个阶段还负责把响应头回写给客户端。
func (p *Player) downloadFirst(w http.ResponseWriter, ctx context.Context) (int64, int64, error) {
	start, end := p.start, p.end
	if end <= 0 {
		// 这里只用来「探测总长度 + 尽快回响应头」，取小一点。
		// 取满 1MB 会让响应头延迟 6~8 秒（光鸭限速下单块就要这么久），
		// ExoPlayer 等不到响应头就超时。
		// 真正的数据由后面的流式循环补齐，所以首块小不影响完整性。
		end = probeSize
	} else {
		// Range 结束位是闭区间，这里转成内部处理更方便的开区间。
		end += 1
	}
	end = start + min(end, p.chunkSize)

	// 首块决定整个响应能否建立，必须多试几轮。
	// 光鸭限流时会连续几次都回 JSON 占位，重试次数给足才能穿过去。
	chunk, header, status, err := p.downloadChunk(ctx, start, end, 8)
	if err != nil {
		return 0, 0, err
	}

	// 光鸭 CDN 会**间歇性忽略 Range 头**，直接回 200 且不带 Content-Range
	// （实测连续 6 次里出现 1 次）。原实现此时直接报错放弃，
	// 于是播放器收到 200 + 零字节，一直卡在 PREPARING 并每 15 秒重试。
	// 这里改为多重回退推断总长度，拿不到再报错。
	var totalLength int64
	if matches := crRegex.FindStringSubmatch(header.Get("Content-Range")); len(matches) == 4 {
		totalLength, _ = strconv.ParseInt(matches[3], 10, 64)
	}
	if totalLength <= 0 && status == http.StatusOK {
		// 200 表示服务端返回了整个文件，Content-Length 就是总长
		if cl := header.Get("Content-Length"); cl != "" {
			totalLength, _ = strconv.ParseInt(cl, 10, 64)
		}
	}
	if totalLength <= 0 {
		// 再试一次带 Range 的探测，换一条链
		if _, h2, _, e2 := p.downloadChunk(ctx, 0, 1, 3); e2 == nil {
			if m2 := crRegex.FindStringSubmatch(h2.Get("Content-Range")); len(m2) == 4 {
				totalLength, _ = strconv.ParseInt(m2[3], 10, 64)
			}
		}
	}
	if totalLength <= 0 {
		return 0, 0, errors.New("未获取到文件总大小")
	}

	// 服务端忽略了 Range 直接回整个文件时，首块里的数据远多于我们要的，
	// 截断到请求区间，避免后续按 chunk 拼接时字节错位。
	if status == http.StatusOK && int64(len(chunk)) > end-start {
		chunk = chunk[:end-start]
	}

	if p.end <= 0 {
		// 原版这里直接拉到文件尾部，于是 Content-Length 声明成整个文件
		// （实测 2.4GB），一次响应必须连续传完 2.4GB。
		// 中途任何一个分块彻底失败就断流，播放器收到的字节少于声明值，
		// 报 CONTAINER_UNSUPPORTED / MANIFEST_MALFORMED。
		// 改为每次只承诺一个窗口，传完正常结束，播放器自己发下一个 Range 续传。
		// 这也是内置 Java 代理能稳定播放而 Go 版不行的关键差异。
		end = start + windowSize - 1
		if end > totalLength-1 {
			end = totalLength - 1
		}
	} else {
		end = p.end
		if end-start+1 > windowSize {
			end = start + windowSize - 1
		}
		if end > totalLength-1 {
			end = totalLength - 1
		}
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

	// Content-Type 固定成 video/mp4，与内置 Java 代理保持一致。
	//
	// 为什么不透传源站的值：光鸭对 mkv 返回 video/x-matroska，
	// 而 ExoPlayer/Media3 拿到它之后既没走 MatroskaExtractor，
	// 也没成功嗅探，日志里报 format=application/octet-stream
	// originalFormat=null，最终 ERROR_CODE_PARSING_CONTAINER_UNSUPPORTED。
	// 内置 Java 代理一直固定回 video/mp4，实测能正常播放 —— 
	// exo 会据此走通用嗅探路径，按实际字节判断容器（mkv 也能正确识别）。
	h.Set("Content-Type", "video/mp4")

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
			// 退避上限 600ms —— 首块重试次数给到 8 次，
			// 若用线性退避总耗时会到 14 秒，播放器早就超时了。
			backoff := time.Duration(retry+1) * 150 * time.Millisecond
			if backoff > 600*time.Millisecond {
				backoff = 600 * time.Millisecond
			}
			select {
			case <-time.After(backoff):
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

	// 光鸭 CDN 在限流/异常时会返回一个 43 字节的 JSON 占位响应
	// {"code":0,"msg":"光鸭云盘为您服务"}，HTTP 200 且 Content-Type: application/json。
	// 这不是视频数据，必须当失败处理并换一条链重试；
	// 原实现把它当正常数据收下，于是播放器拿到 43 字节垃圾，
	// 报 CONTAINER_UNSUPPORTED 并每 15 秒重试。
	if ct := resp.Header.Get("Content-Type"); strings.Contains(strings.ToLower(ct), "json") {
		return nil, nil, -1, fmt.Errorf("源站返回 JSON 占位响应(限流?)，需换链重试")
	}

	// 某些源站即使带了 Range 也会直接返回 200，这里一并兼容。
	if resp.StatusCode == 206 || resp.StatusCode == 200 {
		want := end - start
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, nil, -1, err
		}
		// 光鸭会间歇性忽略 Range 回 200 + 整个文件。
		// 分块阶段拿到这种响应必须截取自己那一段，否则拼接后字节全错位。
		if resp.StatusCode == 200 && start > 0 {
			if int64(len(data)) > start+want {
				data = data[start : start+want]
			} else if int64(len(data)) > start {
				data = data[start:]
			}
		} else if int64(len(data)) > want && want > 0 {
			data = data[:want]
		}
		return data, resp.Header, resp.StatusCode, nil
	}

	return nil, nil, -1, fmt.Errorf("状态码: %d", resp.StatusCode)
}

// 单次响应最多吐这么多。
// 不能一次承诺整个文件：那样中途任一分块失败就会断流，
// 播放器拿到的字节少于 Content-Length，直接报容器解析失败。
const windowSize int64 = 24 * 1024 * 1024

// 首块探测大小。只为拿到 Content-Range 里的总长度并尽快回响应头，
// 取太大会让响应头迟迟不返回（光鸭限速下 1MB 要 6~8 秒），播放器直接超时。
// 64KB 足够拿到响应头，且 mkv/mp4 的头部信息通常也在这个范围内。
const probeSize int64 = 64 * 1024

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
