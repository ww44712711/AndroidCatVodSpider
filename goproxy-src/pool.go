// 多签名链轮转池。
//
// 存在的原因：光鸭网盘是**按签名链限速**的，同一条链上堆并发会被卡在 ~0.8 MB/s
// 并开始返 503；而多条不同签名链各开少量并发可以叠加吞吐。
// 原版 proxy.go 是「单 URL 多线程」，正好命中光鸭的失败模式，所以补上这个池。
//
// 用法：/proxy?url=<逗号分隔的多条链>&thread=12&chunkSize=1024&perUrl=2
package main

import (
	"strings"
	"sync"
	"time"
)

// URLPool 负责在多条等价下载链之间轮转，并限制单条链的并发数。
type URLPool struct {
	mu      sync.Mutex
	urls    []string
	active  []int
	perURL  int
	next    int
	closed  bool
}

// NewURLPool 创建轮转池。perURL <= 0 时退化为不限制。
func NewURLPool(urls []string, perURL int) *URLPool {
	p := &URLPool{
		urls:   urls,
		active: make([]int, len(urls)),
		perURL: perURL,
	}
	return p
}

// ParseURLs 拆分逗号分隔的多条链，并去重、去空。
// 单条链时行为与原版完全一致。
func ParseURLs(raw string) []string {
	parts := strings.Split(raw, ",")
	seen := make(map[string]bool, len(parts))
	out := make([]string, 0, len(parts))
	for _, s := range parts {
		s = strings.TrimSpace(s)
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// Acquire 取一条当前并发未满的链，并原子占位。
// 挑选与占位必须在同一把锁内完成 —— 分成两步会让多个协程同时选中同一条链，
// 一起冲过 perURL 上限，退化成单链多并发的慢速模式（实测吞吐掉到 1/4）。
//
// 重要：这里**绝不能无限等待**。
// 线程数可能大于池子容量（perURL × 链数），例如 thread=8 / perURL=2 / 4 条链，
// 容量刚好 8，一旦有链暂时不可用，多余的协程就会卡在 cond.Wait() 上；
// 而调用方在等这些协程返回，于是整个响应死锁 —— 客户端收到 200 但零字节。
// 所以超时后退化为「轮转返回一条链，不占位」，宁可略微超并发也不能卡死。
func (p *URLPool) Acquire() (int, string) {
	// 关键：**绝不在锁内等待**。
	//
	// 之前用 defer Unlock + cond.Wait 在锁里等空位，而 Release 也要抢同一把锁，
	// 8 个 worker 高频 Acquire/Release 时全部串行化在这把 mutex 上，
	// 实际并行度远低于设定值 —— 实测 8 秒只交付 0.5~2MB（应有 25MB）。
	// 内置 Java 代理是「拿不到就立刻返回，睡一小会儿再试」，没有锁内等待，
	// 这里改成同样的做法。
	deadline := time.Now().Add(3 * time.Second)
	for {
		p.mu.Lock()
		if p.closed {
			s := p.anyLocked()
			p.mu.Unlock()
			return -1, s
		}
		n := len(p.urls)
		if n == 0 {
			p.mu.Unlock()
			return -1, ""
		}
		for i := 0; i < n; i++ {
			idx := (p.next + i) % n
			if p.perURL <= 0 || p.active[idx] < p.perURL {
				p.active[idx]++
				p.next = (idx + 1) % n
				url := p.urls[idx]
				p.mu.Unlock()
				return idx, url
			}
		}
		// 全占满：超时就不占位直接给一条，避免调用方卡死
		if time.Now().After(deadline) {
			idx := p.next % n
			p.next = (idx + 1) % n
			url := p.urls[idx]
			p.mu.Unlock()
			return -1, url
		}
		p.mu.Unlock()
		// 锁外短睡，让 Release 能顺畅拿到锁
		time.Sleep(15 * time.Millisecond)
	}
}

// anyLocked 在已持锁的前提下返回任意一条链。
func (p *URLPool) anyLocked() string {
	if len(p.urls) == 0 {
		return ""
	}
	idx := p.next % len(p.urls)
	p.next = (idx + 1) % len(p.urls)
	return p.urls[idx]
}

// Release 归还占位。
func (p *URLPool) Release(idx int) {
	if idx < 0 {
		return
	}
	p.mu.Lock()
	if p.active[idx] > 0 {
		p.active[idx]--
	}
	p.mu.Unlock()
}

// Size 返回池中链数量。
func (p *URLPool) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.urls)
}

// Close 唤醒所有等待者，避免请求结束后有协程永久阻塞。
func (p *URLPool) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
}

// sizeKey 由链集合派生出稳定的缓存键。
// 签名链每次都不同，但同一文件的 fid 参数一致，用它做键。
func sizeKey(urls []string) string {
	if len(urls) == 0 {
		return ""
	}
	u := urls[0]
	if i := strings.Index(u, "fid="); i >= 0 {
		rest := u[i+4:]
		if j := strings.IndexByte(rest, '&'); j > 0 {
			return rest[:j]
		}
		return rest
	}
	return u
}
