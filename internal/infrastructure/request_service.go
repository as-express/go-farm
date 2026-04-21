package infrastructure

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"time"
)

type RequestService struct {
	proxyManager *ProxyManager
	clients map[string]*http.Client
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
		
		client, ok := s.clients[proxyKey]
		if !ok {
			proxyURL, _ := url.Parse(fmt.Sprintf("http://%s:%s@%s:%s", p.User, p.Pass, p.IP, p.Port))
			transport := &http.Transport{
				Proxy:           http.ProxyURL(proxyURL),
				MaxIdleConns:    100,
				IdleConnTimeout: 90 * time.Second,
			}
			client = &http.Client{
				Transport: transport,
				Timeout:   15 * time.Second,
			}
			s.clients[proxyKey] = client
		}

		if req.Header.Get("User-Agent") == "" {
			req.Header.Set("User-Agent", s.getRandomUserAgent())
		}

		resp, err := client.Do(req.WithContext(ctx))
		if err == nil {
			if resp.StatusCode >= 400 {
				if !s.isRetryableStatus(resp.StatusCode) {
					return resp, fmt.Errorf("terminal error status: %d", resp.StatusCode)
				}
				lastErr = fmt.Errorf("status code: %d", resp.StatusCode)
				resp.Body.Close()
			} else {
				return resp, nil
			}
		} else {
			lastErr = err
		}

		if i < retries {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(500*(i+1)) * time.Millisecond):
			}
		}
	}

	return nil, fmt.Errorf("failed after %d retries: %v", retries, lastErr)
}

func (s *RequestService) isRetryableStatus(status int) bool {
	return status != 401 && status != 404 && status != 403
}

func (s *RequestService) getRandomUserAgent() string {
	agents := []string{
		"Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
		"Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
	}
	return agents[rand.Intn(len(agents))]
}