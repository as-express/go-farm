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
	return &KaspiRepo{
		requestService: rs,
	}
}

func (r *KaspiRepo) GetProductSellers(
	ctx context.Context,
	pID string,
	cID string,
	isExpress bool,
	limit int,
	ignoreIntercity bool,
	promo interface{},
) (*domain.KaspiProductSellersM, error) {
	data := map[string]interface{}{
		"cityId":       cID,
		"id":           pID,
		"limit":        limit,
		"page":         0,
		"sortOption":   "PRICE",
		"installation": false,
	}

	if promo != nil {
		data["product"] = promo
	}

	if ignoreIntercity {
		data["deliveryFilter"] = "TOMORROW"
	}

	if isExpress {
		data["deliveryFilter"] = "EXPRESS"
	}

	body, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("marshal sellers request body failed: %w", err)
	}

	requestURL := "https://kaspi.kz/yml/offer-view/offers/" + pID

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		requestURL,
		bytes.NewReader(body),
	)

	if err != nil {
		return nil, fmt.Errorf("create sellers request failed: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Referer", "https://kaspi.kz/shop/p/p-"+pID)
	req.Header.Set("X-KS-City", cID)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

	resp, err := r.requestService.Request(ctx, req, infrastructure.ProxyBot, 5)
	if err != nil {
		return nil, fmt.Errorf("request service error: %w", err)
	}

	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read sellers response body failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("kaspi returned status %d", resp.StatusCode)
	}

	var result domain.KaspiProductSellersM

	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("unmarshal sellers response failed: %w", err)
	}

	return &result, nil
}

func (r *KaspiRepo) GetProductPromoConditions(
	ctx context.Context,
	pID string,
) (interface{}, error) {
	requestURL := fmt.Sprintf("https://kaspi.kz/shop/p/p-%s?c=750000000", pID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create promo request failed: %w", err)
	}

	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Referer", "https://kaspi.kz/shop/")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

	resp, err := r.requestService.Request(ctx, req, infrastructure.ProxyBot, 3)
	if err != nil {
		return nil, fmt.Errorf("promo request service error: %w", err)
	}

	defer resp.Body.Close()

	html, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read promo response body failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("promo kaspi returned status %d", resp.StatusCode)
	}

	match := promoRegex.FindStringSubmatch(string(html))
	if len(match) < 2 {
		return nil, fmt.Errorf("no promo")
	}

	var itemData struct {
		Card struct {
			PromoConditions interface{} `json:"promoConditions"`
		} `json:"card"`
	}

	if err := json.Unmarshal([]byte(match[1]), &itemData); err != nil {
		return nil, fmt.Errorf("unmarshal promo conditions failed: %w", err)
	}

	return itemData.Card.PromoConditions, nil
}