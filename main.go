package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"

	"demetra-farm/internal/config"
	"demetra-farm/internal/domain"
	"demetra-farm/internal/infrastructure"
	"demetra-farm/internal/metrics"
	"demetra-farm/internal/repository"
	"demetra-farm/internal/usecase"
)

const executeJobs = true

const maxWorkers = 1000
const jobsBufferSize = 50000


func main() {
	log.Println("[INIT] Starting Demetra Farm Monitoring")

	if executeJobs {
		log.Println("[MODE] executeJobs=true, Go will run monitor.Execute")
	} else {
		log.Println("[MODE] executeJobs=false, Go will only receive NATS messages")
	}

	log.Printf(
		"[CONFIG] maxWorkers=%d jobsBufferSize=%d",
		maxWorkers,
		jobsBufferSize,
	)

	appCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.LoadConfig()

	// ----------------------------------------------------
	// REDIS
	// ----------------------------------------------------
	rdbCache := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisCacheAddr,
		Password: cfg.RedisCachePass,
		DB:       0,
	})

	rdbBull := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisBullAddr,
		Password: cfg.RedisBullPass,
		DB:       0,
	})

	if err := rdbCache.Ping(appCtx).Err(); err != nil {
		log.Fatalf("[REDIS_CACHE_ERROR] %v", err)
	}

	if err := rdbBull.Ping(appCtx).Err(); err != nil {
		log.Fatalf("[REDIS_BULL_ERROR] %v", err)
	}

	log.Println("[OK] Redis connected")

	// ----------------------------------------------------
	// NATS
	// ----------------------------------------------------
	nc, err := nats.Connect(
		cfg.NatsURL,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(5*time.Second),
		nats.Name("demetra-farm-go"),
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			if sub != nil {
				log.Printf(
					"[NATS_ASYNC_ERROR] subject=%s queue=%s err=%v",
					sub.Subject,
					sub.Queue,
					err,
				)
				return
			}

			log.Printf("[NATS_ASYNC_ERROR] err=%v", err)
		}),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Printf("[NATS_DISCONNECTED] err=%v", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("[NATS_RECONNECTED] url=%s", nc.ConnectedUrl())
		}),
	)

	if err != nil {
		log.Fatalf("[NATS_ERROR] %v", err)
	}

	defer nc.Close()

	log.Println("[OK] NATS connected")

	// ----------------------------------------------------
	// INIT SERVICES
	// ----------------------------------------------------
	pm := infrastructure.NewProxyManager([]infrastructure.Proxy{
		{
			URL: cfg.ProxyURL,
		},
	})
	rs := infrastructure.NewRequestService(pm)

	cacheRepo := repository.NewRedisRepo(rdbCache)
	taskRepo := repository.NewTaskRepo(rdbBull)
	kaspiRepo := repository.NewKaspiRepo(rs)

	monitor := usecase.NewMonitoringUseCase(
		cacheRepo,
		kaspiRepo,
		taskRepo,
		cfg.FarmName,
	)

	// ----------------------------------------------------
	// COUNTERS
	// ----------------------------------------------------
	var activeWorkers int64
	var receivedCount int64
	var enqueuedCount int64
	var startedCount int64
	var emptyPayloadCount int64
	var badPayloadCount int64
	var bufferFullCount int64

	jobs := make(chan domain.IFarmPayload, jobsBufferSize)

	// ----------------------------------------------------
	// WORKER POOL
	// ----------------------------------------------------
	var workerWg sync.WaitGroup

	if executeJobs {
		for i := 0; i < maxWorkers; i++ {
			workerID := i + 1

			workerWg.Add(1)

			go func() {
				defer workerWg.Done()

				for {
					select {
					case <-appCtx.Done():
						return

					case job, ok := <-jobs:
						if !ok {
							return
						}

						processJob(
							appCtx,
							monitor,
							job,
							workerID,
							&activeWorkers,
							&startedCount,
						)
					}
				}
			}()
		}
	}

	// ----------------------------------------------------
	// METRICS FLUSH
	// ----------------------------------------------------
	go func() {
		ticker := time.NewTicker(55 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-appCtx.Done():
				return

			case <-ticker.C:
				metrics.FlushMetrics(
					context.Background(),
					cacheRepo,
					cfg.FarmName,
				)
			}
		}
	}()

	// ----------------------------------------------------
	// RPS LOGGER
	// ----------------------------------------------------
	var sub *nats.Subscription

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		var prevTasks int64
		var prevSuccess int64
		var prevFail int64
		var prevHTTP int64
		var prevReceived int64
		var prevEnqueued int64
		var prevStarted int64
		var prevEmpty int64
		var prevBad int64
		var prevBufferFull int64

		for {
			select {
			case <-appCtx.Done():
				return

			case <-ticker.C:
				snap := metrics.Metrics.Snapshot()

				tasks := snap["tasks"].(map[string]interface{})
				network := snap["network"].(map[string]interface{})

				all := tasks["all"].(int64)
				success := tasks["success"].(int64)
				fail := tasks["fail"].(int64)

				httpReq := network["requests"].(int64)
				lat := network["latency"].(int64)

				received := atomic.LoadInt64(&receivedCount)
				enqueued := atomic.LoadInt64(&enqueuedCount)
				started := atomic.LoadInt64(&startedCount)
				emptyPayloads := atomic.LoadInt64(&emptyPayloadCount)
				badPayloads := atomic.LoadInt64(&badPayloadCount)
				bufferFull := atomic.LoadInt64(&bufferFullCount)

				taskRPS := calcRPS(all, prevTasks, 5)
				successRPS := calcRPS(success, prevSuccess, 5)
				failRPS := calcRPS(fail, prevFail, 5)
				httpRPS := calcRPS(httpReq, prevHTTP, 5)
				receivedRPS := calcRPS(received, prevReceived, 5)
				enqueuedRPS := calcRPS(enqueued, prevEnqueued, 5)
				startedRPS := calcRPS(started, prevStarted, 5)
				emptyRPS := calcRPS(emptyPayloads, prevEmpty, 5)
				badRPS := calcRPS(badPayloads, prevBad, 5)
				bufferFullRPS := calcRPS(bufferFull, prevBufferFull, 5)

				prevTasks = all
				prevSuccess = success
				prevFail = fail
				prevHTTP = httpReq
				prevReceived = received
				prevEnqueued = enqueued
				prevStarted = started
				prevEmpty = emptyPayloads
				prevBad = badPayloads
				prevBufferFull = bufferFull

				var pendingMsgs int
				var pendingBytes int
				var delivered int64
				var dropped int

				if sub != nil {
					pendingMsgs, pendingBytes, _ = sub.Pending()
					delivered, _ = sub.Delivered()
					dropped, _ = sub.Dropped()
				}

				var m runtime.MemStats
				runtime.ReadMemStats(&m)

				log.Printf(
					"[RPS] mode_execute=%v received=%d/s enqueued=%d/s started=%d/s tasks=%d/s success=%d/s fail=%d/s empty=%d/s bad=%d/s http=%d/s activeWorkers=%d/%d jobsBuffer=%d/%d bufferFull=%d/s goroutines=%d alloc=%dMB heap=%dMB stack=%dMB sys=%dMB avgHttp=%dms pending=%d pendingBytes=%d delivered=%d dropped=%d",
					executeJobs,
					receivedRPS,
					enqueuedRPS,
					startedRPS,
					taskRPS,
					successRPS,
					failRPS,
					emptyRPS,
					badRPS,
					httpRPS,
					atomic.LoadInt64(&activeWorkers),
					maxWorkers,
					len(jobs),
					cap(jobs),
					bufferFullRPS,
					runtime.NumGoroutine(),
					m.Alloc/1024/1024,
					m.HeapAlloc/1024/1024,
					m.StackInuse/1024/1024,
					m.Sys/1024/1024,
					lat,
					pendingMsgs,
					pendingBytes,
					delivered,
					dropped,
				)
			}
		}
	}()

	// ----------------------------------------------------
	// NATS SUBSCRIBE
	// ----------------------------------------------------
	sub, err = nc.QueueSubscribe("farm", "main_queue", func(m *nats.Msg) {
		atomic.AddInt64(&receivedCount, 1)

		var envelope struct {
			Pattern string              `json:"pattern"`
			Data    domain.IFarmPayload `json:"data"`
		}

		if err := json.Unmarshal(m.Data, &envelope); err != nil {
			atomic.AddInt64(&badPayloadCount, 1)
			log.Printf("[BAD_PAYLOAD] err=%v raw=%s", err, string(m.Data))
			return
		}

		payload := envelope.Data

		if payload.ShopID == 0 || payload.ProductID == "" || len(payload.CityIds) == 0 {
			atomic.AddInt64(&emptyPayloadCount, 1)
			return
		}

		// ----------------------------------------------------
		// RECEIVE-ONLY benchmark mode
		// ----------------------------------------------------
		if !executeJobs {
			atomic.AddInt64(&enqueuedCount, 1)
			atomic.AddInt64(&startedCount, 1)

			metrics.Metrics.RecordTask()
			metrics.Metrics.RecordSuccess()
			return
		}

		// ----------------------------------------------------
		// BACKPRESSURE
		// ----------------------------------------------------
		// Если buffer заполнен — callback заблокируется на отправке в jobs.
		// Это нормально: мы не создаём бесконечные goroutines.
		// NATS pending начнёт расти, и это будет честный сигнал, что workers не успевают.
		if len(jobs) == cap(jobs) {
			atomic.AddInt64(&bufferFullCount, 1)
		}

		select {
		case jobs <- payload:
			atomic.AddInt64(&enqueuedCount, 1)

		case <-appCtx.Done():
			return
		}
	})

	if err != nil {
		log.Fatal(err)
	}

	if err := sub.SetPendingLimits(1_000_000, 1<<30); err != nil {
		log.Fatalf("[NATS_PENDING_LIMIT_ERROR] %v", err)
	}

	if err := nc.Flush(); err != nil {
		log.Fatalf("[NATS_FLUSH_ERROR] %v", err)
	}

	log.Println("[LISTENING] subject=farm queue=main_queue")

	// ----------------------------------------------------
	// SHUTDOWN
	// ----------------------------------------------------
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	<-stop

	log.Println("[SHUTDOWN] signal received")

	// Сначала останавливаем получение новых сообщений из NATS.
	if err := nc.Drain(); err != nil {
		log.Printf("[NATS_DRAIN_ERR] %v", err)
	}

	log.Println("[NATS] drained")

	// Закрываем jobs channel, чтобы workers дочитали то, что уже в буфере.
	close(jobs)

	log.Println("[WAITING] workers finishing...")

	done := make(chan struct{})

	go func() {
		workerWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("[DONE] graceful shutdown complete")

	case <-time.After(30 * time.Second):
		log.Println("[FORCE_EXIT] workers timeout 30s")
		cancel()
	}
}

func processJob(
	ctx context.Context,
	monitor *usecase.MonitoringUseCase,
	job domain.IFarmPayload,
	workerID int,
	activeWorkers *int64,
	startedCount *int64,
) {
	atomic.AddInt64(startedCount, 1)
	atomic.AddInt64(activeWorkers, 1)

	defer atomic.AddInt64(activeWorkers, -1)

	metrics.Metrics.RecordTask()

	defer func() {
		if r := recover(); r != nil {
			log.Printf(
				"[PANIC] worker=%d shop=%d product=%s panic=%v",
				workerID,
				job.ShopID,
				job.ProductID,
				r,
			)

			metrics.Metrics.RecordFail()
		}
	}()

	err := monitor.Execute(ctx, job)

	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			log.Printf(
				"[TASK_CANCELLED] worker=%d shop=%d product=%s",
				workerID,
				job.ShopID,
				job.ProductID,
			)
			return
		}

		metrics.Metrics.RecordFail()

		log.Printf(
			"[ERROR] worker=%d shop=%d product=%s err=%v",
			workerID,
			job.ShopID,
			job.ProductID,
			err,
		)

		return
	}

	metrics.Metrics.RecordSuccess()
}

func calcRPS(current int64, prev int64, seconds int64) int64 {
	if current < prev {
		return 0
	}

	return (current - prev) / seconds
}