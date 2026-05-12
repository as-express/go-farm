package usecase

import (
	"context"
	"crypto/md5"
	"demetra-farm/internal/domain"
	"demetra-farm/internal/repository"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"
)

type MonitoringUseCase struct {
	cache    *repository.RedisRepo
	kaspi    *repository.KaspiRepo
	tasks    *repository.TaskRepo
	farmName string
}

func NewMonitoringUseCase(c *repository.RedisRepo, k *repository.KaspiRepo, t *repository.TaskRepo, farmName string) *MonitoringUseCase {
	return &MonitoringUseCase{
		cache:    c,
		kaspi:    k,
		tasks:    t,
		farmName: farmName,
	}
}

func (u *MonitoringUseCase) Execute(ctx context.Context, data domain.IFarmPayload) error {
	if len(data.CityIds) == 0 {
		return nil
	}

	isUseBonusDemping := data.IsUseBonusDemping

	// PromoConditions cache 3 days
	var promoConditions interface{}
	if isUseBonusDemping {
		cacheKey := fmt.Sprintf("CACHE_FARM_PRODUCT_%s", data.ProductID)

		if !u.cache.GetJson(ctx, cacheKey, &promoConditions) {
			promoConditions, _ = u.kaspi.GetProductPromoConditions(ctx, data.ProductID)
			_ = u.cache.SetJson(ctx, cacheKey, promoConditions, 86400*3)
		}
	}

	// FIXED: now city failures are logged, not silent
	sellers := u.getProductSellersByCityIds(ctx, data, promoConditions)

	if len(sellers) == 0 {
		return fmt.Errorf("all city requests failed for shop=%d product=%s", data.ShopID, data.ProductID)
	}

	_ = u.cache.Set(
		ctx,
		fmt.Sprintf("SHOP_PRODUCT_DETOUR_TIME_%d_%s", data.ShopID, data.ProductID),
		fmt.Sprintf("%d", time.Now().UnixMilli()),
		86400,
	)

	priceHash := u.generateDampingHash(sellers, isUseBonusDemping)

	_ = u.cache.SetJson(
		ctx,
		fmt.Sprintf("SHOP_PRODUCT_DETOUR_%d_%s", data.ShopID, data.ProductID),
		sellers,
		86400,
	)

	u.differenceHashDamping(
		ctx,
		data.ShopID,
		data.ProductID,
		priceHash,
		data.IsSpecialDamping,
		len(data.CityIds),
	)

	newHash := u.calculateMd5(sellers)
	diffKey := fmt.Sprintf("DIFF_MONITORING_HASH_%d_%s", data.ShopID, data.ProductID)

	oldHash, _ := u.cache.Get(ctx, diffKey)
	if oldHash == newHash {
		return nil
	}

	_ = u.cache.Set(ctx, diffKey, newHash, 900)
	_ = u.cache.Set(
		ctx,
		fmt.Sprintf("HANDLE_%d_%s", data.ShopID, data.ProductID),
		"В ОБРАБОТКЕ",
		14400,
	)

	return u.tasks.AddTask(data.ShopID, data.ProductID)

}

func (u *MonitoringUseCase) getProductSellersByCityIds(ctx context.Context, data domain.IFarmPayload, promo interface{}) map[string]*domain.KaspiProductSellersM {

	results := make(map[string]*domain.KaspiProductSellersM)

	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, cityID := range data.CityIds {
		wg.Add(1)

		go func(cID string) {
			defer wg.Done()

			ignoreIntercity := data.IsIgnoreIntercityDelivery

			if data.ShopID == 2604 && cID == "750000000" {
				ignoreIntercity = false
			}

			// FIXED HERE
			res, err := u.retryGetSellers(
				ctx,
				data.ProductID,
				cID,
				data.IsExpressMonitoringOn,
				data.SellersLimit,
				ignoreIntercity,
				promo,
			)

			if err != nil {
				log.Printf(
					"[CITY_FAIL] shop=%d product=%s city=%s err=%v",
					data.ShopID,
					data.ProductID,
					cID,
					err,
				)
				return
			}

			if data.IsUseBonusDemping && len(res.Offers) > 0 {
				sort.Slice(res.Offers, func(i, j int) bool {
					return u.resolvePrice(res.Offers[i], true) <
						u.resolvePrice(res.Offers[j], true)
				})
			}

			mu.Lock()
			results[cID] = res
			mu.Unlock()

		}(cityID)
	}

	wg.Wait()
	return results

}

// FIXED VERSION (no silent fail anymore)
func (u *MonitoringUseCase) retryGetSellers(
	ctx context.Context,
	pID string,
	cID string,
	express bool,
	limit int,
	ignore bool,
	promo interface{},
) (*domain.KaspiProductSellersM, error) {
	cacheKey := fmt.Sprintf(
		"CACHE_MONITORING_CITY_%s_%s_%d",
		pID,
		cID,
		limit,
	)

	var cachedRes domain.KaspiProductSellersM

	if u.cache.GetJson(ctx, cacheKey, &cachedRes) {
		
		return &cachedRes, nil
	}

	var lastErr error

	retryCount := 2

	for retryCount > 0 {

		

		res, err := u.kaspi.GetProductSellers(
			ctx,
			pID,
			cID,
			express,
			limit,
			ignore,
			promo,
		)


		if err == nil && res != nil {
			

			u.incrementSuccess(ctx)

			ttl := u.calculateDynamicTTL(ctx, pID, cID, res)
			_ = u.cache.SetJson(ctx, cacheKey, res, ttl)

			return res, nil
		}

		lastErr = err


		retryCount--

	
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// 1v1 как Nest:
		// sleep нет, retry сразу
	}


	return nil, fmt.Errorf(
		"retryGetSellers failed product=%s city=%s after 2 retries: %w",
		pID,
		cID,
		lastErr,
	)
}

func (u *MonitoringUseCase) incrementSuccess(ctx context.Context) {
	successKey := fmt.Sprintf("SUCCESS_FARM_%s", u.farmName)
	_ = u.cache.Incr(ctx, successKey, 3600)
}

func (u *MonitoringUseCase) calculateDynamicTTL(ctx context.Context, pID, cID string, data *domain.KaspiProductSellersM) int {

	diffKey := fmt.Sprintf("DIFFERENCE_MONITORING_CITY_%s_%s", pID, cID)

	type diffCache struct {
		Hash      string `json:"hash"`
		NotChange int    `json:"notChange"`
	}

	var dc diffCache
	u.cache.GetJson(ctx, diffKey, &dc)

	newHash := u.calculateMd5(data)

	if dc.Hash == "" {
		dc.Hash = newHash
		dc.NotChange = 0
	} else {
		if dc.Hash == newHash {
			dc.NotChange++
		} else {
			dc.Hash = newHash
			dc.NotChange = 0
		}
	}

	_ = u.cache.SetJson(ctx, diffKey, dc, 86400*3)

	ttl := 51

	if dc.NotChange > 10 {
		ttl = int(math.Round(float64(dc.NotChange*60) / 1.5))
	}

	if ttl < 51 {
		ttl = 51
	}

	if ttl > 3600 {
		ttl = 3600
	}

	return ttl

}

func (u *MonitoringUseCase) generateDampingHash(sellers map[string]*domain.KaspiProductSellersM, useBonus bool) string {

	keys := make([]string, 0, len(sellers))
	for k := range sellers {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {
		iv, _ := strconv.Atoi(keys[i])
		jv, _ := strconv.Atoi(keys[j])
		return iv < jv
	})

	hash := ""

	for _, k := range keys {
		hash += fmt.Sprintf("%s:", k)

		for _, o := range sellers[k].Offers {
			hash += fmt.Sprintf("%.0f-", u.resolvePrice(o, useBonus))
		}
	}

	return hash

}

func (u *MonitoringUseCase) differenceHashDamping(
	ctx context.Context,
	sID int,
	pID string,
	hashStr string,
	spec bool,
	shopCount int,
) {
	key := fmt.Sprintf("DIFF_HASH_DAMPING_%d_%s", sID, pID)
	dataHash := fmt.Sprintf("%x", md5.Sum([]byte(hashStr)))

	var cache domain.DampingCache
	found := u.cache.GetJson(ctx, key, &cache)

	if !found || cache.Hash == "" {
		cache.Hash = dataHash
		cache.NotChange = 0
		cache.DetourCount = 0
		cache.DampingLevel = "unknown"

		_ = u.cache.SetJson(ctx, key, cache, 86400*10)
		return
	}

	cache.DetourCount++

	if cache.Hash != dataHash {
		cache.Hash = dataHash
		cache.NotChange = 0

		_ = u.cache.ZAdd(
			ctx,
			fmt.Sprintf("MONITORING_TASKS_%d", sID),
			float64(time.Now().UnixMilli()+60000),
			pID,
		)
	} else {
		cache.NotChange++
	}

	cache.DampingLevel = u.getDampingLevel(
		cache.DetourCount,
		cache.NotChange,
		sID,
		pID,
		spec,
	)

	if shopCount <= 15 {
		_ = u.cache.ZAdd(
			ctx,
			fmt.Sprintf("MONITORING_TASKS_%d", sID),
			float64(time.Now().UnixMilli()),
			pID,
		)
	}

	_ = u.cache.SetJson(ctx, key, cache, 86400*10)
}

func (u *MonitoringUseCase) getDampingLevel(detour, notChange, sID int, pID string, spec bool) string {

	if sID == 2571 && pID == "122031169" {
		return "high"
	}

	if spec {
		return "high"
	}

	specialPartners := []int{2571, 3547, 3284}

	isSpecial := false
	for _, id := range specialPartners {
		if id == sID {
			isSpecial = true
			break
		}
	}

	if detour < 6 {
		return "unknown"
	}

	if detour >= 6 && notChange <= 50 {
		return "high"
	}

	if notChange > 7 && notChange < 200 && isSpecial {
		return "high"
	}

	if notChange > 7 && notChange < 20 && !isSpecial {
		return "medium"
	}

	if notChange >= 12 && notChange < 30 {
		return "low"
	}

	if notChange >= 30 && notChange < 50 {
		return "veryLow"
	}

	if notChange >= 50 {
		return "none"
	}

	return "unknown"

}

func (u *MonitoringUseCase) resolvePrice(offer domain.KaspiSeller, isBonus bool) float64 {

	if isBonus && offer.PriceMinusBonus > 0 {
		return offer.PriceMinusBonus
	}

	return offer.Price

}

func (u *MonitoringUseCase) calculateMd5(data interface{}) string {
	b, _ := json.Marshal(data)
	return fmt.Sprintf("%x", md5.Sum(b))
}
