package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type TaskRepo struct {
	rdb *redis.Client
}

func NewTaskRepo(rdb *redis.Client) *TaskRepo {
	return &TaskRepo{rdb: rdb}
}

// FULL FIXED VERSION
// Поведение максимально близко к Nest BullService.addTask()
func (r *TaskRepo) AddTask(shopID int, productID string) error {
	ctx := context.Background()

	queueName := "DEBOUNCE_CHANGE_QUEUE"
	jobID := fmt.Sprintf("%d_%s", shopID, productID)

	key := fmt.Sprintf("bull:%s:%s", queueName, jobID)

	nowMs := time.Now().UnixMilli()
	timeToRemove := int64(1000 * 120 * 60) // 120 min как в Nest

	// ---------------------------------------------------
	// 1. Проверяем старый job
	// ---------------------------------------------------
	exists, err := r.rdb.Exists(ctx, key).Result()
	if err != nil {
		return err
	}

	if exists > 0 {
		jobData, err := r.rdb.HGetAll(ctx, key).Result()
		if err == nil && len(jobData) > 0 {

			shouldRemove := false

			// failed job
			if jobData["failedReason"] != "" {
				shouldRemove = true
			}

			// finished job
			if jobData["finishedOn"] != "" {
				shouldRemove = true
			}

			// stale job
			if tsRaw := jobData["timestamp"]; tsRaw != "" {
				if ts, parseErr := strconv.ParseInt(tsRaw, 10, 64); parseErr == nil {
					if ts < nowMs-timeToRemove {
						shouldRemove = true
					}
				}
			}

			if shouldRemove {
				pipe := r.rdb.TxPipeline()

				pipe.Del(ctx, key)

				pipe.LRem(
					ctx,
					fmt.Sprintf("bull:%s:wait", queueName),
					0,
					jobID,
				)

				pipe.LRem(
					ctx,
					fmt.Sprintf("bull:%s:active", queueName),
					0,
					jobID,
				)

				pipe.LRem(
					ctx,
					fmt.Sprintf("bull:%s:paused", queueName),
					0,
					jobID,
				)

				pipe.ZRem(
					ctx,
					fmt.Sprintf("bull:%s:delayed", queueName),
					jobID,
				)

				pipe.ZRem(
					ctx,
					fmt.Sprintf("bull:%s:completed", queueName),
					jobID,
				)

				pipe.ZRem(
					ctx,
					fmt.Sprintf("bull:%s:failed", queueName),
					jobID,
				)

				_, _ = pipe.Exec(ctx)

			} else {
				// живой job уже существует = как Nest return
				return nil
			}
		} else {
			return nil
		}
	}

	// ---------------------------------------------------
	// 2. Создаем новый reqId как в Nest
	// ---------------------------------------------------
	timestampSec := time.Now().Unix()
	reqID := fmt.Sprintf("b_%d_%s", timestampSec, jobID)

	payload := map[string]interface{}{
		"shopId":    shopID,
		"productId": productID,
		"reqId":     reqID,
	}

	jobJson, _ := json.Marshal(payload)

	// ---------------------------------------------------
	// 3. Создаем новый Bull Job
	// ---------------------------------------------------
	pipe := r.rdb.TxPipeline()

	pipe.HSet(ctx, key, map[string]interface{}{
		"name":             "__default__",
		"data":             string(jobJson),
		"opts":             `{"attempts":10,"removeOnComplete":true,"removeOnFail":true}`,
		"timestamp":        nowMs,
		"delay":            0,
		"priority":         0,
		"processedOn":      "",
		"finishedOn":       "",
		"failedReason":     "",
		"attemptsMade":     0,
		"stacktrace":       "[]",
		"returnvalue":      "",
	})

	pipe.LPush(
		ctx,
		fmt.Sprintf("bull:%s:wait", queueName),
		jobID,
	)

	pipe.Publish(
		ctx,
		fmt.Sprintf("bull:%s:waiting", queueName),
		jobID,
	)

	_, err = pipe.Exec(ctx)

	return err
}