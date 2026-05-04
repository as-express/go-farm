package repository

import (
	"context"
	"encoding/json"
	"regexp"
	"time"

	"github.com/redis/go-redis/v9"
)

var shopIdRegex = regexp.MustCompile(`^CACHE_SHOP_PRODUCT_(\d+)_`)

type RedisRepo struct {
	client *redis.Client
}

func NewRedisRepo(client *redis.Client) *RedisRepo {
	return &RedisRepo{client: client}
}

func (r *RedisRepo) GetJson(ctx context.Context, key string, target interface{}) bool {
	match := shopIdRegex.FindStringSubmatch(key)
	if len(match) > 1 {
		exists, _ := r.client.Exists(ctx, "DEL_"+match[1]).Result()
		if exists > 0 { return false }
	}

	val, err := r.client.Get(ctx, key).Result()
	if err != nil { return false }
	return json.Unmarshal([]byte(val), target) == nil
}

func (r *RedisRepo) SetJson(ctx context.Context, key string, data interface{}, ttlSeconds int) error {
	val, _ := json.Marshal(data)
	return r.client.Set(ctx, key, val, time.Duration(ttlSeconds)*time.Second).Err()
}

func (r *RedisRepo) Set(ctx context.Context, key string, val string, ttlSeconds int) error {
    return r.client.Set(ctx, key, val, time.Duration(ttlSeconds)*time.Second).Err()
}

func (r *RedisRepo) Get(ctx context.Context, key string) (string, error) {
    // Возвращаем пустую строку если ключа нет, чтобы не было паники
    val, err := r.client.Get(ctx, key).Result()
    if err == redis.Nil {
        return "", nil
    }
    return val, err
}

func (r *RedisRepo) ZAdd(ctx context.Context, key string, score float64, member string) error {
    err := r.client.ZAdd(ctx, key, redis.Z{Score: score, Member: member}).Err()
    if err == nil {
        r.client.Expire(ctx, key, 24*time.Hour) // Как в NestJS: multi.expire(key, 24h)
    }
    return err
}

func (r *RedisRepo) Incr(ctx context.Context, key string, ttlSeconds int) error {
	pipe := r.client.Pipeline()
	pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, time.Duration(ttlSeconds)*time.Second)
	_, err := pipe.Exec(ctx)
	return err
}