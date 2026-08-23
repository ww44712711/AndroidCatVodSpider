package main

// Android 上的 DNS 解析。
//
// 为什么必须自己实现：
// 纯 Go 编译（CGO_ENABLED=0）用的是 Go 自带的 DNS 解析器，它读 /etc/resolv.conf
// 拿上游 DNS 地址。但 **Android 系统没有 /etc/resolv.conf** —— 系统 DNS 由 netd
// 通过 Bionic 的 API 提供，纯 Go 拿不到。找不到配置时 Go 会回落到 localhost:53，
// 而那里没有 DNS 服务，于是报：
//   dial tcp: lookup xxx on [::1]:53: read udp [::1]:53: connection refused
// 结果每个分块都解析失败、重试耗尽，表现就是「一点速度都没有」。
//
// 解决办法：跳过系统解析，直接对公共 DNS 发 UDP 查询，并缓存结果。

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

// 公共 DNS，按顺序尝试。国内优先，避免境外 DNS 被干扰。
var dnsServers = []string{
	"223.5.5.5:53",   // 阿里
	"119.29.29.29:53", // 腾讯
	"114.114.114.114:53",
	"8.8.8.8:53",
	"1.1.1.1:53",
}

type dnsEntry struct {
	ips    []string
	expire time.Time
}

var (
	dnsMu    sync.RWMutex
	dnsCache = map[string]dnsEntry{}
)

const dnsTTL = 5 * time.Minute

// resolveHost 返回域名对应的 IPv4 列表，带缓存。
func resolveHost(host string) ([]string, error) {
	// 本身就是 IP 就直接用
	if ip := net.ParseIP(host); ip != nil {
		return []string{host}, nil
	}

	dnsMu.RLock()
	if e, ok := dnsCache[host]; ok && time.Now().Before(e.expire) && len(e.ips) > 0 {
		ips := e.ips
		dnsMu.RUnlock()
		return ips, nil
	}
	dnsMu.RUnlock()

	// 先试系统解析：有的 ROM 确实提供了 /etc/resolv.conf，能用就用
	if addrs, err := net.DefaultResolver.LookupHost(context.Background(), host); err == nil {
		var v4 []string
		for _, a := range addrs {
			if ip := net.ParseIP(a); ip != nil && ip.To4() != nil {
				v4 = append(v4, a)
			}
		}
		if len(v4) > 0 {
			putDNS(host, v4)
			return v4, nil
		}
	}

	// 系统解析不可用 —— 自己发 UDP 查询
	var lastErr error
	for _, srv := range dnsServers {
		ips, err := queryA(srv, host)
		if err == nil && len(ips) > 0 {
			putDNS(host, ips)
			return ips, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no answer")
	}
	return nil, fmt.Errorf("解析 %s 失败: %v", host, lastErr)
}

func putDNS(host string, ips []string) {
	dnsMu.Lock()
	if len(dnsCache) > 256 {
		dnsCache = map[string]dnsEntry{}
	}
	dnsCache[host] = dnsEntry{ips: ips, expire: time.Now().Add(dnsTTL)}
	dnsMu.Unlock()
}

// queryA 手写一个最小的 DNS A 记录查询（RFC 1035）。
// 不依赖任何系统配置，直接对指定 DNS 服务器发 UDP 包。
func queryA(server, host string) ([]string, error) {
	conn, err := net.DialTimeout("udp", server, 3*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))

	id := uint16(rand.Intn(65535))
	msg := buildQuery(id, host)
	if _, err := conn.Write(msg); err != nil {
		return nil, err
	}
	buf := make([]byte, 1500)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	return parseAnswer(buf[:n], id)
}

func buildQuery(id uint16, host string) []byte {
	var b []byte
	b = append(b, byte(id>>8), byte(id)) // ID
	b = append(b, 0x01, 0x00)            // 标准查询，需要递归
	b = append(b, 0x00, 0x01)            // QDCOUNT = 1
	b = append(b, 0x00, 0x00)            // ANCOUNT
	b = append(b, 0x00, 0x00)            // NSCOUNT
	b = append(b, 0x00, 0x00)            // ARCOUNT
	for _, label := range strings.Split(strings.TrimSuffix(host, "."), ".") {
		if len(label) == 0 || len(label) > 63 {
			continue
		}
		b = append(b, byte(len(label)))
		b = append(b, label...)
	}
	b = append(b, 0x00)       // 域名结束
	b = append(b, 0x00, 0x01) // QTYPE = A
	b = append(b, 0x00, 0x01) // QCLASS = IN
	return b
}

func parseAnswer(msg []byte, id uint16) ([]string, error) {
	if len(msg) < 12 {
		return nil, fmt.Errorf("响应过短")
	}
	if uint16(msg[0])<<8|uint16(msg[1]) != id {
		return nil, fmt.Errorf("ID 不匹配")
	}
	if rcode := msg[3] & 0x0f; rcode != 0 {
		return nil, fmt.Errorf("rcode=%d", rcode)
	}
	qd := int(uint16(msg[4])<<8 | uint16(msg[5]))
	an := int(uint16(msg[6])<<8 | uint16(msg[7]))
	if an == 0 {
		return nil, fmt.Errorf("无记录")
	}

	off := 12
	// 跳过问题段
	for i := 0; i < qd; i++ {
		var err error
		off, err = skipName(msg, off)
		if err != nil {
			return nil, err
		}
		off += 4 // QTYPE + QCLASS
	}

	var ips []string
	for i := 0; i < an && off < len(msg); i++ {
		var err error
		off, err = skipName(msg, off)
		if err != nil {
			return nil, err
		}
		if off+10 > len(msg) {
			break
		}
		typ := uint16(msg[off])<<8 | uint16(msg[off+1])
		rdlen := int(uint16(msg[off+8])<<8 | uint16(msg[off+9]))
		off += 10
		if off+rdlen > len(msg) {
			break
		}
		if typ == 1 && rdlen == 4 { // A 记录
			ips = append(ips, net.IP(msg[off:off+4]).String())
		}
		off += rdlen
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("无 A 记录")
	}
	return ips, nil
}

// skipName 跳过 DNS 报文里的域名（含压缩指针）。
func skipName(msg []byte, off int) (int, error) {
	for {
		if off >= len(msg) {
			return 0, fmt.Errorf("越界")
		}
		l := int(msg[off])
		if l == 0 {
			return off + 1, nil
		}
		if l&0xc0 == 0xc0 { // 压缩指针，占 2 字节
			return off + 2, nil
		}
		off += l + 1
	}
}

// dialContext 用自定义解析结果建立连接。
// 把它装进 http.Transport 就能绕开 Android 缺失的系统 DNS 配置。
func dialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	ips, err := resolveHost(host)
	if err != nil {
		return nil, err
	}
	d := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	var lastErr error
	for _, ip := range ips {
		conn, err := d.DialContext(ctx, network, net.JoinHostPort(ip, port))
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("无可用地址")
	}
	return nil, lastErr
}
