package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"

	"demetra-farm/internal/domain"
	"demetra-farm/internal/infrastructure"
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

type sellersRequestPayload struct {
	CityID              string      `json:"cityId"`
	ID                  string      `json:"id"`
	MerchantUID         []string    `json:"merchantUID"`
	Limit               int         `json:"limit"`
	Page                int         `json:"page"`
	SortOption          string      `json:"sortOption"`
	HighRating          interface{} `json:"highRating"`
	SearchText          interface{} `json:"searchText"`
	IsExcellentMerchant interface{} `json:"isExcellentMerchant"`
	ZoneID              []string    `json:"zoneId"`
	DeliveryFilter      string      `json:"deliveryFilter,omitempty"`
	Product             interface{} `json:"product,omitempty"`
}

func (r *KaspiRepo) GetProductSellers(
	pID, cID string,
	isExpress bool,
	limit int,
	ignoreIntercity bool,
	promo interface{},
) (*domain.KaspiProductSellersM, error) {
	data := sellersRequestPayload{
		CityID:      cID,
		ID:          pID,
		MerchantUID: []string{},
		Limit:       limit,
		Page:        0,
		SortOption:  "PRICE",
		Product:     promo,
	}

	if cID == "750000000" {
		data.ZoneID = []string{"Magnum_ZONE1"}
	}
	if ignoreIntercity {
		data.DeliveryFilter = "TOMORROW"
	}
	if isExpress {
		data.DeliveryFilter = "EXPRESS"
	}

	body, _ := json.Marshal(data)

	u := "https://kaspi.kz/yml/offer-view/offers/" + pID
	req, err := http.NewRequest("POST", u, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("X-KS-City", cID)
	req.Header.Set("Referer", fmt.Sprintf("https://kaspi.kz/shop/p/%s/", pID))
	req.Header.Set("Cookie", fmt.Sprintf("is_mobile_app=true; kaspi.storefront.cookie.city=%s", cID))

	resp, err := r.requestService.Request(context.Background(), req, infrastructure.ProxyBot, 5)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result domain.KaspiProductSellersM
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (r *KaspiRepo) GetProductPromoConditions(pID string) (interface{}, error) {
	u := fmt.Sprintf("https://kaspi.kz/shop/p/p-%s?c=750000000", pID)
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	resp, err := r.requestService.Request(context.Background(), req, infrastructure.ProxyBot, 3)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	htmlBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	match := promoRegex.FindStringSubmatch(string(htmlBytes))
	if len(match) < 2 {
		return nil, fmt.Errorf("promo conditions not found in html for product %s", pID)
	}

	var itemData struct {
		Card struct {
			PromoConditions interface{} `json:"promoConditions"`
		} `json:"card"`
	}

	if err := json.Unmarshal([]byte(match[1]), &itemData); err != nil {
		return nil, err
	}

	return itemData.Card.PromoConditions, nil
}