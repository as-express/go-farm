package infrastructure

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type DedupeService struct {
	rdb *redis.Client

	mu       sync.Mutex
	inflight map[string]struct{}
}

func NewDedupeService(rdb *redis.Client) *DedupeService {
	return &DedupeService{
		rdb:      rdb,
		inflight: make(map[string]struct{}),
	}
}

// true => можно выполнять
// false => duplicate
func (d *DedupeService) Acquire(
	ctx context.Context,
	shopID int,
	productID string,
) bool {

	key := fmt.Sprintf("%d:%s", shopID, productID)

	// ---------------- LOCAL LOCK ----------------
	d.mu.Lock()

	if _, exists := d.inflight[key]; exists {
		d.mu.Unlock()
		return false
	}

	d.inflight[key] = struct{}{}
	d.mu.Unlock()

	// ---------------- REDIS LOCK ----------------
	redisKey := "farm_lock:" + key

	ok, err := d.rdb.SetNX(
		ctx,
		redisKey,
		"1",
		20*time.Second,
	).Result()

	if err != nil {
		// если redis временно упал — оставляем local lock
		return true
	}

	if !ok {
		d.Release(ctx, shopID, productID)
		return false
	}

	return true
}

func (d *DedupeService) Release(
	ctx context.Context,
	shopID int,
	productID string,
) {
	key := fmt.Sprintf("%d:%s", shopID, productID)

	d.mu.Lock()
	delete(d.inflight, key)
	d.mu.Unlock()

	_ = d.rdb.Del(ctx, "farm_lock:"+key).Err()
}