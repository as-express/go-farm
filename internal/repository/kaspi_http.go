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

func getProductSellersCookie(cityID string, hasPromo bool) string {
	if hasPromo && (cityID == "392010000" || cityID == "511010000") {
		return fmt.Sprintf(
			"_ga=GA1.1.998307943.1718173357; "+
				"JSESSIONID=64899D9FA0D535FD2B29C5E75566D927; "+
				"deviceId=49053654-CE1B-4F99-9DB0-4C38067DFF3C; "+
				"installId=265673E3-9813-4CF1-869D-5CB357A28CCC; "+
				"is_mobile_app=true; "+
				"kaspi-payment-region=18; "+
				"locale=ru-RU; "+
				"ma_bld=987; "+
				"ma_platform_type=IOS; "+
				"ma_platform_ver=26.3.1; "+
				"ma_ver=5.116; "+
				"mobapp_version=38; "+
				"opay=true; "+
				"pd=JfXGf2dLyodG8HLaSd46zA; "+
				"userType=PRIVILEGED_USER; "+
				"kaspi.storefront.cookie.city=%s;",
			cityID,
		)
	}

	return fmt.Sprintf(
		"is_mobile_app=true; "+
			"kaspi-payment-region=18; "+
			"locale=ru-RU; "+
			"ma_bld=948; "+
			"ma_platform_type=IOS; "+
			"ma_platform_ver=26.3; "+
			"ma_ver=5.110; "+
			"mobapp_version=38; "+
			"opay=true; "+
			"opay_gold_processing=true; "+
			"opay_processing=true; "+
			"opay_red_processing=true; "+
			"pd=6s9D1NEVZhUH1EX4shVSBm; "+
			"userType=PRIVILEGED_USER; "+
			"kaspi.storefront.cookie.city=%s; "+
			"ks.tg=24",
		cityID,
	)
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
		"cityId":               cID,
		"id":                   pID,
		"merchantUID":          []string{},
		"limit":                limit,
		"page":                 0,
		"sortOption":           "PRICE",
		"highRating":           nil,
		"searchText":           nil,
		"isExcellentMerchant":  nil,
		"zoneId":               nil,
	}

	if cID == "750000000" {
		data["zoneId"] = []string{"Magnum_ZONE1"}
	}

	if ignoreIntercity {
		data["deliveryFilter"] = "TOMORROW"
	}

	if isExpress {
		data["deliveryFilter"] = "EXPRESS"
	}

	if promo != nil {
		data["product"] = promo
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

	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Accept", "application/json, text/*")
	req.Header.Set("Accept-Language", "ru")
	req.Header.Set("User-Agent", "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148")
	req.Header.Set("Referer", "https://kaspi.kz/shop/p/"+pID+"/?referrer=desktop_QR")
	req.Header.Set("Origin", "https://kaspi.kz")
	req.Header.Set("X-KS-City", cID)
	req.Header.Set("X-Flexible-Express-Enabled", "true")
	req.Header.Set("X-Description-Enabled", "true")
	req.Header.Set("Cookie", getProductSellersCookie(cID, promo != nil))

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
		return nil, fmt.Errorf("kaspi returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result domain.KaspiProductSellersM

	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("unmarshal sellers response failed: %w, body: %s", err, string(respBody))
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
		return nil, fmt.Errorf("promo kaspi returned status %d: %s", resp.StatusCode, string(html))
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