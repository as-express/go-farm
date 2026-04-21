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

func (r *RedisRepo) extractShopId(key string) string {
    match := shopIdRegex.FindStringSubmatch(key)
    if len(match) > 1 {
        return match[1]
    }
    return ""
}

func (r *RedisRepo) GetJson(ctx context.Context, key string, target interface{}) bool {
    shopID := r.extractShopId(key)
    if shopID != "" {
        exists, _ := r.client.Exists(ctx, "DEL_"+shopID).Result()
        if exists > 0 {
            return false
        }
    }

    val, err := r.client.Get(ctx, key).Result()
    if err != nil { return false }

    return json.Unmarshal([]byte(val), target) == nil
}

func (r *RedisRepo) SetJson(ctx context.Context, key string, data interface{}, ttl int) error {
    val, err := json.Marshal(data)
    if err != nil {
        return fmt.Errorf("marshal error: %w", err)
    }
    
    pipe := r.client.Pipeline()
    pipe.Set(ctx, key, val, 0)
    if ttl > 0 {
        pipe.Expire(ctx, key, time.Duration(ttl)*time.Second)
    }
    _, err = pipe.Exec(ctx)
    return err
}
func (r *RedisRepo) ZAdd(ctx context.Context, key string, score float64, member string) error {
    pipe := r.client.Pipeline()
    pipe.ZAdd(ctx, key, redis.Z{Score: score, Member: member})
    pipe.Expire(ctx, key, 24*time.Hour)
    _, err := pipe.Exec(ctx)
    return err
}

func (r *RedisRepo) Set(ctx context.Context, key string, val string, ttl int) error {
    return r.client.Set(ctx, key, val, time.Duration(ttl)*time.Second).Err()
}

func (r *RedisRepo) Get(ctx context.Context, key string) (string, error) {
    return r.client.Get(ctx, key).Result()
}