package domain

type KaspiSeller struct {
    MerchantID      string `json:"merchantId"`
    MerchantName    string `json:"merchantName"`
    Price           float64    `json:"price"`
    PriceMinusBonus float64    `json:"priceMinusBonus"`
    LocatedInCity   string `json:"locatedInCity"`
    KaspiDelivery   bool   `json:"kaspiDelivery"`
}

type KaspiProductSellersM struct {
    Total       int           `json:"total"`
    OffersCount int           `json:"offersCount"`
    Offers      []KaspiSeller `json:"offers"`
}

type IFarmPayload struct {
    ShopID                    int      `json:"shopId"`
    ProductID                 string   `json:"productId"`
    CityIds                   []string `json:"cityIds"`
    IsSpecialDamping          bool     `json:"isSpecialDamping"`
    IsIgnoreIntercityDelivery bool     `json:"isIgnoreIntercityDelivery"`
    IsExpressMonitoringOn     bool     `json:"isExpressMonitoringOn"`
    SellersLimit              int      `json:"sellersLimit"`
    IsUseBonusDemping         bool     `json:"isUseBonusDemping"`
}

type DampingCache struct {
    Hash         string `json:"hash"`
    NotChange    int    `json:"notChange"`
    DetourCount  int    `json:"detourCount"`
    DampingLevel string `json:"dampingLevel"`
}