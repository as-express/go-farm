package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	RedisCacheAddr string
	RedisCachePass string
	RedisBullAddr  string
	RedisBullPass  string
	NatsURL        string
	FarmName       string
}

func LoadConfig() *Config {
	err := godotenv.Load()
	if err != nil {
		log.Println("Warning: .env file not found, using system env")
	}

	return &Config{
		RedisCacheAddr: getEnv("REDIS_CACHE_ADDR", "localhost:6379"),
		RedisCachePass: getEnv("REDIS_CACHE_PASS", ""),
		RedisBullAddr:  getEnv("REDIS_BULL_ADDR", "localhost:6379"),
		RedisBullPass:  getEnv("REDIS_BULL_PASS", ""),
		NatsURL:        getEnv("NATS_URL", "nats://127.0.0.1:4222"),
		FarmName:       getEnv("FARM_NAME", "GO_FARM_DEFAULT"),
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}