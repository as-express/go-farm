package metrics

import (
	"context"
	"demetra-farm/internal/repository"
	"fmt"
)

func FlushMetrics(
	ctx context.Context,
	repo *repository.RedisRepo,
	farm string,
) {
	data := Metrics.Snapshot()

	key := fmt.Sprintf("metrics:instance:%s", farm)

	_ = repo.SetJson(ctx, key, data, 600)

	Metrics.Reset()
}