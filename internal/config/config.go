package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	PORT                    string
	DB_HOST                 string
	JWTSecret               string
	CLOUDINARY_URL          string
	REDIS_URL               string
	MailjetAPIKey           string
	MailjetSecretKey        string
	MailjetFromEmail        string
	MailjetFromName         string
	FirebaseCredentialsPath string
}

func Load() (Config, error) {
	// Loads .env for local dev. On Render/prod the file won't exist and
	// the real env vars are already set, so we ignore the error.
	_ = godotenv.Load()

	cfg := Config{
		// Render injects PORT automatically; default keeps local dev working.
		PORT:                    getOrDefault("PORT", "8080"),
		FirebaseCredentialsPath: os.Getenv("FIREBASE_CREDENTIALS_PATH"),
	}

	// Every var below is required. Collect them all so one deploy log
	// shows every missing key instead of only the first.
	required := map[string]*string{
		"DB_HOST":            &cfg.DB_HOST,
		"JWTSecret":          &cfg.JWTSecret,
		"CLOUDINARY_URL":     &cfg.CLOUDINARY_URL,
		"REDIS_URL":          &cfg.REDIS_URL,
		"MAILJET_API_KEY":    &cfg.MailjetAPIKey,
		"MAILJET_SECRET_KEY": &cfg.MailjetSecretKey,
		"MAILJET_FROM_EMAIL": &cfg.MailjetFromEmail,
		"MAILJET_FROM_NAME":  &cfg.MailjetFromName,
	}

	var missing []string
	for key, dest := range required {
		val := strings.TrimSpace(os.Getenv(key))
		if val == "" {
			missing = append(missing, key)
			continue
		}
		*dest = val
	}

	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing required environment variables: %s",
			strings.Join(missing, ", "))
	}

	return cfg, nil
}

func getOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}