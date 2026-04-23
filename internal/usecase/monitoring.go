package usecase

import (
	"context"
	"crypto/md5"
	"demetra-farm/internal/domain"
	"demetra-farm/internal/repository"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)

type MonitoringUseCase struct {
	cache *repository.RedisRepo
	kaspi *repository.KaspiRepo
	tasks *repository.TaskRepo
}

func NewMonitoringUseCase(c *repository.RedisRepo, k *repository.KaspiRepo, t *repository.TaskRepo) *MonitoringUseCase {
	return &MonitoringUseCase{cache: c, kaspi: k, tasks: t}
}

func (u *MonitoringUseCase) Execute(ctx context.Context, data domain.IFarmPayload) error {
	log.Printf("[%s] Starting monitoring for %d cities", data.ProductID, len(data.CityIds))
	if len(data.CityIds) == 0 || data.ShopID != 312 { return nil }

	var promoConditions interface{}
	if data.IsUseBonusDemping {
		cacheKey := fmt.Sprintf("CACHE_FARM_PRODUCT_%s", data.ProductID)
		if !u.cache.GetJson(ctx, cacheKey, &promoConditions) {
			promoConditions, _ = u.kaspi.GetProductPromoConditions(ctx, data.ProductID)
			u.cache.SetJson(ctx, cacheKey, promoConditions, 86400)
		}
	}

	sellersByCity := u.getSellersParallel(ctx, data, promoConditions)
	log.Printf("[%s] Received data from %d/%d cities", data.ProductID, len(sellersByCity), len(data.CityIds))

	// Хэширование и демппинг
	priceHash := u.generatePriceHash(sellersByCity, data.IsUseBonusDemping)
	u.differenceHashDamping(ctx, data.ShopID, data.ProductID, priceHash, data.IsSpecialDamping, len(data.CityIds))

	newHash := u.calculateMd5(sellersByCity)
	diffKey := fmt.Sprintf("DIFF_MONITORING_HASH_%d_%s", data.ShopID, data.ProductID)
	oldHash, _ := u.cache.Get(ctx, diffKey)

	if oldHash == newHash {
		log.Printf("[SKIP] %s - No changes", data.ProductID)
		return nil
	}

	u.cache.Set(ctx, diffKey, newHash, 900)
	err := u.tasks.AddTask(data.ShopID, data.ProductID)

	if err != nil {
        return err
    }
    
    log.Printf("[%s] TASK SENT: Product added to price-updater queue (FARM_TASKS_%d)", data.ProductID, data.ShopID)
    return nil
}

func (u *MonitoringUseCase) getSellersParallel(ctx context.Context, data domain.IFarmPayload, promo interface{}) map[string]*domain.KaspiProductSellersM {
	results := make(map[string]*domain.KaspiProductSellersM)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, cityID := range data.CityIds {
		wg.Add(1)
		go func(cID string) {
			defer wg.Done()
			res, err := u.kaspi.GetProductSellers(ctx, data.ProductID, cID, data.IsExpressMonitoringOn, data.SellersLimit, data.IsIgnoreIntercityDelivery, promo)
			if err == nil {
				mu.Lock()
				results[cID] = res
				mu.Unlock()
			}
		}(cityID)
	}
	wg.Wait()
	return results
}

func (u *MonitoringUseCase) generatePriceHash(sellers map[string]*domain.KaspiProductSellersM, useBonus bool) string {
	keys := make([]string, 0, len(sellers))
	for k := range sellers { keys = append(keys, k) }
	sort.Strings(keys)
	h := ""
	for _, k := range keys {
		for _, o := range sellers[k].Offers {
			p := o.Price
			if useBonus && o.PriceMinusBonus > 0 { p = o.PriceMinusBonus }
			h += fmt.Sprintf("%d|", p)
		}
	}
	return h
}

func (u *MonitoringUseCase) calculateMd5(data interface{}) string {
	b, _ := json.Marshal(data)
	return fmt.Sprintf("%x", md5.Sum(b))
}

func (u *MonitoringUseCase) differenceHashDamping(ctx context.Context, sID int, pID string, hash string, spec bool, count int) {
    key := fmt.Sprintf("DIFF_HASH_DAMPING_%d_%s", sID, pID)
    var cache domain.DampingCache
    u.cache.GetJson(ctx, key, &cache)
    
    cache.DetourCount++
    if cache.Hash != hash {
        cache.Hash = hash
        cache.NotChange = 0
        u.cache.ZAdd(ctx, fmt.Sprintf("MONITORING_TASKS_%d", sID), float64(time.Now().UnixMilli()+60000), pID)
    } else {
        cache.NotChange++
    }
    u.cache.SetJson(ctx, key, cache, 86400*10)
}
