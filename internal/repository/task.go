package repository

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type TaskRepo struct {
	client *redis.Client
}

func NewTaskRepo(client *redis.Client) *TaskRepo {
	return &TaskRepo{
		client: client,
	}
}

// AddTask добавляет ID товара в очередь на обновление цен
func (r *TaskRepo) AddTask(shopID int, productID string) error {
	ctx := context.Background()
	
	// Используем ключ очереди, который ожидает твой основной воркер
	// Например, "FARM_TASKS_{shopID}"
	queueKey := fmt.Sprintf("FARM_TASKS_%d", shopID)
	
	// Добавляем в Set, чтобы избежать дублей в очереди на один и тот же товар
	err := r.client.SAdd(ctx, queueKey, productID).Err()
	if err != nil {
		return fmt.Errorf("failed to add task to redis: %w", err)
	}
	
	return nil
}