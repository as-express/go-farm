package main

import (
	"context"
	"demetra-farm/internal/domain"
	"demetra-farm/internal/infrastructure"
	"demetra-farm/internal/repository"
	"demetra-farm/internal/usecase"
	"encoding/json"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
)

func main() {
	log.Println("[INIT] Starting Demetra Farm Monitoring (Go Edition)...")
	ctx := context.Background()

	// 1. Подключение к основному Redis (CACHE_HOST из твоего конфига)
	// Используем данные: 89.207.255.61:6379
	rdb := redis.NewClient(&redis.Options{
		Addr:     "89.207.255.61:6379",
		Password: "iSx6Pi7QjhQRqFPgf6n9", // Твой пароль из конфига
		DB:       0,                // Обычно 0, если не указано иное
	})

	// Проверка связи
	err := rdb.Ping(ctx).Err()
	if err != nil {
		log.Fatalf("[ERROR] Redis connection failed: %v", err)
	}
	log.Println("[OK] Redis connected (89.207.255.61)")

	// 2. Подключение к NATS
	natsUrl := "nats://demetra:farmdemetrapassword@77.246.247.230:4222"
	nc, err := nats.Connect(natsUrl,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(5*time.Second),
	)
	if err != nil {
		log.Fatalf("[ERROR] NATS connection failed: %v", err)
	}
	log.Println("[OK] NATS connected")
	defer nc.Close()

	// 3. Инициализация слоев
	pm := infrastructure.NewProxyManager()
	rs := infrastructure.NewRequestService(pm)
	cacheRepo := repository.NewRedisRepo(rdb)
	kaspiRepo := repository.NewKaspiRepo(rs)
	taskRepo := repository.NewTaskRepo(rdb)

	monitor := usecase.NewMonitoringUseCase(cacheRepo, kaspiRepo, taskRepo)

	// 4. Настройка подписки
	subject := "farm"
	queueGroup := "main_queue"

	log.Printf("[LISTENING] Waiting for tasks on subject: %s...", subject)

	_, err = nc.QueueSubscribe(subject, queueGroup, func(m *nats.Msg) {
		// Обертка для NestJS (NestJS часто шлет данные в поле "data")
		var envelope struct {
			Data domain.IFarmPayload `json:"data"`
		}

		// Сначала пробуем распаковать с оберткой "data" (как делает NestJS)
		if err := json.Unmarshal(m.Data, &envelope); err != nil {
			// Если не вышло, пробуем напрямую в Payload
			var directPayload domain.IFarmPayload
			if err := json.Unmarshal(m.Data, &directPayload); err != nil {
				log.Printf("[ERROR] Failed to unmarshal NATS data: %v", err)
				return
			}
			envelope.Data = directPayload
		}

		payload := envelope.Data
		log.Printf("[RECEIVED] Task: Product %s, Shop %d", payload.ProductID, payload.ShopID)

		// Ограничение как в Semaphore(150), чтобы не положить сервер
		go func(p domain.IFarmPayload) {
			if err := monitor.Execute(ctx, p); err != nil {
				log.Printf("[ERROR] Execute failed for %s: %v", p.ProductID, err)
			}
		}(payload)
	})

	if err != nil {
		log.Fatal(err)
	}

	select {}
}