package infrastructure

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type ProxyType string

const ProxyBot ProxyType = "bot"

const debugHTTPLogs = false

type Proxy struct {
	URL string
}

type ProxyManager struct {
	proxies []Proxy
}

func NewProxyManager(proxies []Proxy) *ProxyManager {
	valid := make([]Proxy, 0, len(proxies))

	for _, p := range proxies {
		if p.URL == "" {
			continue
		}

		valid = append(valid, p)
	}

	return &ProxyManager{
		proxies: valid,
	}
}

func (pm *ProxyManager) GetByType(pt ProxyType) Proxy {
	if len(pm.proxies) == 0 {
		return Proxy{}
	}

	return pm.proxies[rand.Intn(len(pm.proxies))]
}

type RequestService struct {
	proxyManager *ProxyManager
	clients      map[string]*http.Client
	mu           sync.RWMutex
}

func NewRequestService(pm *ProxyManager) *RequestService {
	return &RequestService{
		proxyManager: pm,
		clients:      make(map[string]*http.Client),
	}
}

type cancelReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelReadCloser) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}

func (s *RequestService) Request(
	ctx context.Context,
	req *http.Request,
	pType ProxyType,
	retries int,
) (*http.Response, error) {
	var lastErr error

	var bodyBytes []byte
	if req.Body != nil {
		var err error

		bodyBytes, err = io.ReadAll(req.Body)
		_ = req.Body.Close()

		if err != nil {
			return nil, fmt.Errorf("read request body failed: %w", err)
		}
	}

	for attempt := 0; attempt <= retries; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		p := s.proxyManager.GetByType(pType)
		client := s.getClient(p)

		attemptCtx, cancel := context.WithTimeout(ctx, 15*time.Second)

		currentReq, err := s.cloneRequestWithContext(req, attemptCtx, bodyBytes)
		if err != nil {
			cancel()
			return nil, err
		}

		if currentReq.Header.Get("User-Agent") == "" {
			currentReq.Header.Set("User-Agent", s.getRandomUserAgent())
		}

		if debugHTTPLogs {
			fmt.Printf(
				"[HTTP_ATTEMPT] try=%d method=%s url=%s proxy=%s\n",
				attempt+1,
				req.Method,
				req.URL.String(),
				hideProxyPassword(p.URL),
			)
		}

		start := time.Now()
		resp, err := client.Do(currentReq)
		duration := time.Since(start)

		if err == nil && resp != nil && resp.StatusCode < 400 {
			if debugHTTPLogs {
				fmt.Printf(
					"[HTTP_OK] try=%d status=%d ms=%d url=%s\n",
					attempt+1,
					resp.StatusCode,
					duration.Milliseconds(),
					req.URL.String(),
				)
			}

			resp.Body = &cancelReadCloser{
				ReadCloser: resp.Body,
				cancel:    cancel,
			}

			return resp, nil
		}

		if resp != nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()

			cancel()

			lastErr = fmt.Errorf("status code: %d", resp.StatusCode)

			if debugHTTPLogs {
				fmt.Printf(
					"[HTTP_BAD_STATUS] try=%d status=%d ms=%d url=%s\n",
					attempt+1,
					resp.StatusCode,
					duration.Milliseconds(),
					req.URL.String(),
				)
			}
		} else {
			cancel()

			if err == nil {
				err = errors.New("empty response")
			}

			lastErr = err

			if debugHTTPLogs {
				fmt.Printf(
					"[HTTP_ERR] try=%d ms=%d url=%s err=%v\n",
					attempt+1,
					duration.Milliseconds(),
					req.URL.String(),
					err,
				)
			}
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}

	return nil, fmt.Errorf("all retries failed: %w", lastErr)
}

func (s *RequestService) cloneRequestWithContext(
	req *http.Request,
	ctx context.Context,
	bodyBytes []byte,
) (*http.Request, error) {
	var body io.Reader

	if bodyBytes != nil {
		body = bytes.NewReader(bodyBytes)
	}

	currentReq, err := http.NewRequestWithContext(
		ctx,
		req.Method,
		req.URL.String(),
		body,
	)

	if err != nil {
		return nil, err
	}

	for k, vv := range req.Header {
		for _, v := range vv {
			currentReq.Header.Add(k, v)
		}
	}

	return currentReq, nil
}

func (s *RequestService) getClient(p Proxy) *http.Client {
	key := p.URL
	if key == "" {
		key = "NO_PROXY"
	}

	s.mu.RLock()
	client, ok := s.clients[key]
	s.mu.RUnlock()

	if ok {
		return client
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	client, ok = s.clients[key]
	if ok {
		return client
	}

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   8 * time.Second,
			KeepAlive: 60 * time.Second,
		}).DialContext,

		MaxIdleConns:        10000,
		MaxIdleConnsPerHost: 1000,
		MaxConnsPerHost:     1000,

		IdleConnTimeout:       120 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,

		ForceAttemptHTTP2: true,
	}

	if p.URL != "" {
		proxyURL, err := url.Parse(p.URL)
		if err != nil {
			panic(fmt.Sprintf("invalid proxy url: %v", err))
		}

		transport.Proxy = http.ProxyURL(proxyURL)
	}

	client = &http.Client{
		Transport: transport,
	}

	s.clients[key] = client

	if debugHTTPLogs {
		fmt.Printf("[HTTP_CLIENT_CREATED] proxy=%s\n", hideProxyPassword(p.URL))
	}

	return client
}

func (s *RequestService) getRandomUserAgent() string {
	agents := []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
	}

	return agents[rand.Intn(len(agents))]
}

func hideProxyPassword(raw string) string {
	if raw == "" {
		return ""
	}

	u, err := url.Parse(raw)
	if err != nil {
		return "INVALID_PROXY_URL"
	}

	if u.User != nil {
		username := u.User.Username()
		u.User = url.UserPassword(username, "***")
	}

	return u.String()
}