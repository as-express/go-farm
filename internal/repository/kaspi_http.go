package repository

import (
	"bytes"
	"context"
	"demetra-farm/internal/domain"
	"demetra-farm/internal/infrastructure"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
)

var promoRegex = regexp.MustCompile(`BACKEND\.components\.item\s*=\s*({[\s\S]*?});?\s*<\/script>`)

type KaspiRepo struct {
	requestService *infrastructure.RequestService
}

func NewKaspiRepo(rs *infrastructure.RequestService) *KaspiRepo {
	return &KaspiRepo{requestService: rs}
}

func (r *KaspiRepo) GetProductSellers(ctx context.Context, pID, cID string, isExpress bool, limit int, ignoreIntercity bool, promo interface{}) (*domain.KaspiProductSellersM, error) {
	data := map[string]interface{}{
		"cityId": cID,
		"id":     pID,
		"limit":  limit,
		"page":   0,
		"sortOption": "PRICE",
		"product":    promo,
	}

	if ignoreIntercity { data["deliveryFilter"] = "TOMORROW" }
	if isExpress { data["deliveryFilter"] = "EXPRESS" }

	body, _ := json.Marshal(data)
	u := "https://kaspi.kz/yml/offer-view/offers/" + pID
	req, _ := http.NewRequest("POST", u, bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-KS-City", cID)

	resp, err := r.requestService.Request(ctx, req, infrastructure.ProxyBot, 5)
	if err != nil { return nil, err }
	defer resp.Body.Close()

	var result domain.KaspiProductSellersM
	json.NewDecoder(resp.Body).Decode(&result)
	return &result, nil
}

func (r *KaspiRepo) GetProductPromoConditions(ctx context.Context, pID string) (interface{}, error) {
	u := fmt.Sprintf("https://kaspi.kz/shop/p/p-%s?c=750000000", pID)
	req, _ := http.NewRequest("GET", u, nil)
	resp, err := r.requestService.Request(ctx, req, infrastructure.ProxyBot, 3)
	if err != nil { return nil, err }
	defer resp.Body.Close()

	html, _ := io.ReadAll(resp.Body)
	match := promoRegex.FindStringSubmatch(string(html))
	if len(match) < 2 { return nil, fmt.Errorf("no promo") }

	var itemData struct {
		Card struct { PromoConditions interface{} `json:"promoConditions"` } `json:"card"`
	}
	json.Unmarshal([]byte(match[1]), &itemData)
	return itemData.Card.PromoConditions, nil
}