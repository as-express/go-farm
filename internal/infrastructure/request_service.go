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

// Для benchmark лучше false.
// Если надо дебажить конкретный request — временно поставь true.
const debugHTTPLogs = false

type Proxy struct {
	IP   string
	Port string
	User string
	Pass string
}

type ProxyManager struct {
	proxies []Proxy
}
func NewProxyManager() *ProxyManager {
	return &ProxyManager{
		proxies: []Proxy{
			{
				IP:   "res-unlimited-ef41714c.plainproxies.com",
				Port: "8080",
				User: "IPNz1S83Ei",
				Pass: "anZI8uGb8WpS3rZ",
			},
		},
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

// cancelReadCloser нужен, чтобы не вызывать cancel()
// до того, как caller прочитал resp.Body.
//
// Иначе flow плохой:
// client.Do() получил headers
// cancel() отменил context
// caller потом читает body
// body может быть уже cancelled
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

		// Было 8s. Для честного сравнения с Nest лучше не резать слишком жёстко.
		// Если proxy/Kaspi иногда отвечает 9-12s, старый код убивал запрос.
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
				"[HTTP_ATTEMPT] try=%d method=%s url=%s proxy=%s:%s\n",
				attempt+1,
				req.Method,
				req.URL.String(),
				p.IP,
				p.Port,
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

			// ВАЖНО:
			// cancel будет вызван только когда caller сделает resp.Body.Close().
			// У тебя в KaspiRepo есть defer resp.Body.Close(), значит ок.
			resp.Body = &cancelReadCloser{
				ReadCloser: resp.Body,
				cancel:    cancel,
			}

			return resp, nil
		}

		if resp != nil {
			// Важно drain body перед close, чтобы Transport мог переиспользовать connection.
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

		// Для benchmark 1-в-1 с Nest retry делаем сразу.
		// В production можно вернуть backoff только для rate-limit/proxy-many-requests.
		//
		// Было:
		// time.Sleep(time.Duration(attempt+1) * 300 * time.Millisecond)
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
	key := fmt.Sprintf("%s:%s@%s:%s", p.User, p.Pass, p.IP, p.Port)

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

	proxyRawURL := fmt.Sprintf(
		"http://%s:%s@%s:%s",
		url.QueryEscape(p.User),
		url.QueryEscape(p.Pass),
		p.IP,
		p.Port,
	)

	proxyURL, err := url.Parse(proxyRawURL)
	if err != nil {
		// Лучше явно создать client без proxy не надо.
		// Если proxy URL сломан — это конфигурационная ошибка.
		panic(fmt.Sprintf("invalid proxy url: %v", err))
	}

	transport := &http.Transport{
		Proxy: http.ProxyURL(proxyURL),

		DialContext: (&net.Dialer{
			Timeout:   8 * time.Second,
			KeepAlive: 60 * time.Second,
		}).DialContext,

		// Старые 100/50 могут быть маленькие при 300+ goroutines и city parallel requests.
		MaxIdleConns:        10000,
		MaxIdleConnsPerHost: 1000,

		// Ограничивает одновременные connections к proxy host.
		// Для теста можно 1000.
		// Если fail/proxy many requests растёт — уменьши до 300-500.
		MaxConnsPerHost: 1000,

		IdleConnTimeout:       120 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,

		ForceAttemptHTTP2: true,
	}

	client = &http.Client{
		Transport: transport,
	}

	s.clients[key] = client

	if debugHTTPLogs {
		fmt.Printf("[HTTP_CLIENT_CREATED] proxy=%s:%s\n", p.IP, p.Port)
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