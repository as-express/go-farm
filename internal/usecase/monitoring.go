package usecase

import (
    "context"
    "crypto/md5"
    "demetra-farm/internal/domain"
    "encoding/json"
    "fmt"
    "log"
    "sort"
    "sync"
    "time"
)

type MonitoringUseCase struct {
    cache repository.RedisRepo
    kaspi repository.KaspiRepo
    tasks repository.TaskRepo
}

func (u *MonitoringUseCase) Execute(ctx context.Context, data domain.IFarmPayload) error {
    if len(data.CityIds) == 0 || data.ShopID != 312 {
        return nil
    }

    log.Printf("[FARM_RECEIVE] Shop %d Prod %s", data.ShopID, data.ProductID)

    var promoConditions interface{}
    if data.IsUseBonusDemping {
        cacheKey := fmt.Sprintf("CACHE_FARM_PRODUCT_%s", data.ProductID)
        if !u.cache.GetJson(ctx, cacheKey, &promoConditions) {
            promoConditions, _ = u.kaspi.GetProductPromoConditions(data.ProductID)
            u.cache.SetJson(ctx, cacheKey, promoConditions, 3600*24*3)
        }
    }

    sellersByCity := u.getProductSellersByCityIds(ctx, data, promoConditions)

    u.cache.Set(ctx, fmt.Sprintf("SHOP_PRODUCT_DETOUR_TIME_%d_%s", data.ShopID, data.ProductID), 
        fmt.Sprintf("%d", time.Now().UnixMilli()), 3600*24)

    priceHash := u.generatePriceHash(sellersByCity, data.IsUseBonusDemping)
    
    u.cache.SetJson(ctx, fmt.Sprintf("SHOP_PRODUCT_DETOUR_%d_%s", data.ShopID, data.ProductID), sellersByCity, 3600*24)
    u.differenceHashDamping(ctx, data.ShopID, data.ProductID, priceHash, data.IsSpecialDamping, len(data.CityIds))

    diffKey := fmt.Sprintf("DIFFERENCE_HASH_HANDLE_BY_MONITORING_%d_%s", data.ShopID, data.ProductID)
    oldHash, _ := u.cache.Get(ctx, diffKey)
    newHash := u.calculateMd5(sellersByCity)

    if oldHash == newHash {
        log.Printf("[FARM_SKIP] Shop %d Prod %s: Prices stable", data.ShopID, data.ProductID)
        return nil
    }

    log.Printf("[FARM_DECISION] Shop %d Prod %s: CHANGES DETECTED", data.ShopID, data.ProductID)
    u.cache.Set(ctx, diffKey, newHash, 900)
    
    return u.tasks.AddTask(data.ShopID, data.ProductID)
}

func (u *MonitoringUseCase) retry(ctx context.Context, pID, cID string, exp, ign bool, lim int, promo interface{}, att int) (*domain.KaspiProductSellersM, error) {
    cacheKey := fmt.Sprintf("CACHE_MONITORING_CITY_%s_%s_%d", pID, cID, lim)
    var cached domain.KaspiProductSellersM
    if u.cache.GetJson(ctx, cacheKey, &cached) { return &cached, nil }

    var lastErr error
    for i := 0; i < att; i++ {
        res, err := u.kaspi.GetProductSellers(ctx, pID, cID, exp, lim, ign, promo)
        if err == nil {
            ttl := u.getDifferenceTTL(ctx, pID, cID, res)
            u.cache.SetJson(ctx, cacheKey, res, ttl)
            return res, nil
        }
        lastErr = err
        
        select {
        case <-ctx.Done():
            return nil, ctx.Err()
        case <-time.After(200 * time.Millisecond):
        }
    }
    return nil, lastErr
}

func (u *MonitoringUseCase) retry(ctx context.Context, pID, cID string, exp, ign bool, lim int, promo interface{}, att int) (*domain.KaspiProductSellersM, error) {
    cacheKey := fmt.Sprintf("CACHE_MONITORING_CITY_%s_%s_%d", pID, cID, lim)
    var cached domain.KaspiProductSellersM
    if u.cache.GetJson(ctx, cacheKey, &cached) { return &cached, nil }

    var lastErr error
    for i := 0; i < att; i++ {
        res, err := u.kaspi.GetProductSellers(pID, cID, exp, lim, ign, promo)
        if err == nil {
            ttl := u.getDifferenceTTL(ctx, pID, cID, res)
            u.cache.SetJson(ctx, cacheKey, res, ttl)
            return res, nil
        }
        lastErr = err
        time.Sleep(200 * time.Millisecond)
    }
    return nil, lastErr
}

func (u *MonitoringUseCase) generatePriceHash(sellers map[string]*domain.KaspiProductSellersM, useBonus bool) string {
    keys := make([]string, 0, len(sellers))
    for k := range sellers { keys = append(keys, k) }
    sort.Strings(keys)
    
    var h string
    for _, k := range keys {
        h += k + ":"
        for _, o := range sellers[k].Offers {
            p := o.Price
            if useBonus && o.PriceMinusBonus > 0 { p = o.PriceMinusBonus }
            h += fmt.Sprintf("%d-", p)
        }
    }
    return h
}

func (u *MonitoringUseCase) calculateMd5(data interface{}) string {
    b, _ := json.Marshal(data)
    return fmt.Sprintf("%x", md5.Sum(b))
}

func (u *MonitoringUseCase) getDifferenceTTL(ctx context.Context, pID, cID string, data interface{}) int {
    key := fmt.Sprintf("DIFF_MONITORING_%s_%s", pID, cID)
    var cache struct { Hash string; NotChange int }
    u.cache.GetJson(ctx, key, &cache)
    
    newHash := u.calculateMd5(data)
    if cache.Hash == newHash { cache.NotChange++ } else { cache.Hash = newHash; cache.NotChange = 0 }
    
    u.cache.SetJson(ctx, key, cache, 86400*3)
    
    ttl := 51
    if cache.NotChange > 10 { ttl = int(float64(cache.NotChange*60) / 1.5) }
    if ttl > 3600 { ttl = 3600 }
    if ttl < 51 { ttl = 51 }
    return ttl
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
    
    cache.DampingLevel = u.getDampingLevel(cache.DetourCount, cache.NotChange, sID, pID, spec)
    if count <= 15 {
        u.cache.ZAdd(ctx, fmt.Sprintf("MONITORING_TASKS_%d", sID), float64(time.Now().UnixMilli()), pID)
    }
    u.cache.SetJson(ctx, key, cache, 86400*10)
}

func (u *MonitoringUseCase) getDampingLevel(detourCount, notChange, shopID int, productID string, isSpecial bool) string {
    if (shopID == 2571 && productID == "122031169") || isSpecial {
        return "high"
    }

    specialPartners := []int{2571, 3547, 3284}
    isSpecialPartner := false
    for _, id := range specialPartners {
        if id == shopID { isSpecialPartner = true; break }
    }

    if detourCount < 6 { return "unknown" }
    if detourCount >= 6 && notChange <= 50 { return "high" }
    
    if notChange > 7 && notChange < 200 && isSpecialPartner { return "high" }
    
    if notChange > 7 && notChange < 20 && !isSpecialPartner { return "medium" }
    
    if notChange >= 12 && notChange < 30 { return "low" }
    if notChange >= 30 && notChange < 50 { return "veryLow" }
    if notChange >= 50 { return "none" }

    return "unknown"
}