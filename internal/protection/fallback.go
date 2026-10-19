package protection

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// FallbackLimiter is a documented per-process fixed-window rate limiter keyed
// by the trusted client IP. It bounds abuse-sensitive routes when Arcjet is
// disabled and when the hosted provider errors while FailClosed is false.
type FallbackLimiter struct {
	mu      sync.Mutex
	limit   int
	proxies []*net.IPNet
	now     func() time.Time
	buckets map[string]*fallbackBucket
}

type fallbackBucket struct {
	window time.Time
	count  int
}

const (
	fallbackWindow      = time.Minute
	fallbackMaxTrackers = 4096
)

// NewFallbackLimiter creates a limiter allowing limit requests per minute per IP.
func NewFallbackLimiter(limit int, proxies []*net.IPNet) *FallbackLimiter {
	if limit <= 0 {
		limit = defaultFallbackPerMinute
	}
	return &FallbackLimiter{limit: limit, proxies: proxies, buckets: make(map[string]*fallbackBucket)}
}

// Deny reports whether the request exceeds the fallback budget.
func (l *FallbackLimiter) Deny(r *http.Request) bool {
	if l == nil || r == nil {
		return false
	}
	now := time.Now()
	if l.now != nil {
		now = l.now()
	}
	window := now.Truncate(fallbackWindow)
	key := ClientIP(r, l.proxies)
	if key == "" {
		key = "unknown"
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	bucket, ok := l.buckets[key]
	if !ok || bucket.window != window {
		if !ok && len(l.buckets) >= fallbackMaxTrackers {
			l.buckets = make(map[string]*fallbackBucket, fallbackMaxTrackers)
		}
		l.buckets[key] = &fallbackBucket{window: window, count: 1}
		return false
	}
	bucket.count++
	return bucket.count > l.limit
}

// ClientIP returns the request source IP without trusting arbitrary headers.
// X-Forwarded-For is honoured only when the direct peer (RemoteAddr) matches a
// configured trusted proxy; otherwise RemoteAddr is used.
func ClientIP(r *http.Request, proxies []*net.IPNet) string {
	if r == nil {
		return ""
	}
	peer := stripPort(strings.TrimSpace(r.RemoteAddr))
	peerIP := net.ParseIP(peer)
	if len(proxies) > 0 && peerIP != nil && trustedPeer(peerIP, proxies) {
		if forwarded := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); forwarded != "" {
			parts := strings.Split(forwarded, ",")
			for i := len(parts) - 1; i >= 0; i-- {
				candidate := stripPort(strings.TrimSpace(parts[i]))
				if ip := net.ParseIP(candidate); ip != nil {
					if ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
						continue
					}
					return ip.String()
				}
			}
			for _, part := range parts {
				candidate := stripPort(strings.TrimSpace(part))
				if ip := net.ParseIP(candidate); ip != nil {
					return ip.String()
				}
			}
		}
	}
	if peerIP != nil {
		return peerIP.String()
	}
	return ""
}

func trustedPeer(ip net.IP, proxies []*net.IPNet) bool {
	for _, network := range proxies {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

func stripPort(hostport string) string {
	if host, _, err := net.SplitHostPort(hostport); err == nil {
		return strings.TrimSpace(host)
	}
	return strings.Trim(hostport, "[]")
}

func parseTrustedProxies(raw []string) ([]*net.IPNet, error) {
	var out []*net.IPNet
	for _, item := range raw {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if strings.Contains(item, "/") {
			_, network, err := net.ParseCIDR(item)
			if err != nil {
				return nil, errProtectionMisconfigured
			}
			out = append(out, network)
			continue
		}
		ip := net.ParseIP(item)
		if ip == nil {
			return nil, errProtectionMisconfigured
		}
		bits := 128
		if ip.To4() != nil {
			bits = 32
		}
		out = append(out, &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)})
	}
	return out, nil
}

func validateKey(key string) error {
	key = strings.TrimSpace(key)
	if len(key) < 16 || !strings.HasPrefix(key, "aj_") {
		return errProtectionMisconfigured
	}
	return nil
}
