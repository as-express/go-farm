package infrastructure

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type ProxyType string
const ProxyBot ProxyType = "bot"

type Proxy struct {
	IP, Port, User, Pass string
}

type ProxyManager struct {
    proxies []Proxy
}

func NewProxyManager() *ProxyManager {
    return &ProxyManager{
        proxies: []Proxy{
            {IP: "res-unlimited-2f24f0ac.plainproxies.com", Port: "8080", User: "1lL6veUUJP", Pass: "4yf62Ko5ti5kimZ"},
        },
    }
}

func (pm *ProxyManager) GetByType(pt ProxyType) Proxy {
    return pm.proxies[rand.Intn(len(pm.proxies))]
}
type RequestService struct {
	proxyManager *ProxyManager
	clients      map[string]*http.Client
	mu           sync.RWMutex // Добавлено для безопасности
}

func NewRequestService(pm *ProxyManager) *RequestService {
	return &RequestService{
		proxyManager: pm,
		clients:      make(map[string]*http.Client),
	}
}

func (s *RequestService) Request(ctx context.Context, req *http.Request, pType ProxyType, retries int) (*http.Response, error) {
	var lastErr error

	for i := 0; i <= retries; i++ {
		p := s.proxyManager.GetByType(pType)
		proxyKey := fmt.Sprintf("%s:%s", p.IP, p.Port)

		s.mu.RLock()
		client, ok := s.clients[proxyKey]
		s.mu.RUnlock()

		if !ok {
			s.mu.Lock()
			// Double check
			if client, ok = s.clients[proxyKey]; !ok {
				proxyURL, _ := url.Parse(fmt.Sprintf("http://%s:%s@%s:%s", p.User, p.Pass, p.IP, p.Port))
				client = &http.Client{
					Transport: &http.Transport{
						Proxy: http.ProxyURL(proxyURL),
						MaxIdleConns: 100,
						IdleConnTimeout: 90 * time.Second,
					},
					Timeout: 15 * time.Second,
				}
				s.clients[proxyKey] = client
			}
			s.mu.Unlock()
		}

		if req.Header.Get("User-Agent") == "" {
			req.Header.Set("User-Agent", s.getRandomUserAgent())
		}

		resp, err := client.Do(req.WithContext(ctx))
		if err != nil {
			log.Printf("[PROXY_WARN] Attempt %d failed: %v", i+1, err)
			lastErr = err
			continue
		}
	
		if err == nil {
			if resp.StatusCode >= 400 {
				resp.Body.Close()
				lastErr = fmt.Errorf("status code: %d", resp.StatusCode)
				if resp.StatusCode == 401 || resp.StatusCode == 403 || resp.StatusCode == 404 {
					return nil, lastErr
				}
			} else {
				return resp, nil
			}
		} else {
			lastErr = err
		}

		time.Sleep(time.Duration(200*(i+1)) * time.Millisecond)
	}
	return nil, lastErr
}

func (s *RequestService) getRandomUserAgent() string {
	agents := []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0 Safari/537.36",
		"Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15",
	}
	return agents[rand.Intn(len(agents))]
}