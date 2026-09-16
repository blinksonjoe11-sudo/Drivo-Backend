package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	PORT    string
	DB_HOST string
	DB_PASSWORD      string
	DB_USER          string
	DB_NAME          string
	DB_PORT          string
	JWTSecret               string
	CLOUDINARY_URL          string
	REDIS_URL               string
	MailjetAPIKey           string
	MailjetSecretKey        string
	MailjetFromEmail        string
	MailjetFromName         string
	FirebaseCredentialsPath string `env:"FIREBASE_CREDENTIALS_PATH"`
}

func Load() (Config, error) {

	godotenv.Load()

	port, err := extractText("PORT")

	if err != nil {
		return Config{}, fmt.Errorf("Port cant be empty")
	}

	db_host, err := extractText("DB_HOST")

	if err != nil {
		return Config{}, fmt.Errorf("cannot find host")
	}

	db_user, err := extractText("DB_USER")

	if err != nil {
		return Config{}, fmt.Errorf("cannot find name")
	}

	db_port, err := extractText("DB_PORT")

	if err != nil {
		return Config{}, fmt.Errorf("cannot find PORT")
	}

	db_name, err := extractText("DB_NAME")

	if err != nil {
		return Config{}, fmt.Errorf("cannot find name")
	}

	db_pass, err := extractText("DB_PASSWORD")

	if err != nil {
		return Config{}, fmt.Errorf("cannot find pass")
	}

	jwtSecret, err := extractText("JWTSecret")

	if err != nil {
		return Config{}, fmt.Errorf("cannot find host")
	}

	cloudinary, err := extractText("CLOUDINARY_URL")

	if err != nil {
		return Config{}, fmt.Errorf("cloudinary not found")
	}

	redisURL, err := extractText("REDIS_URL")

	if err != nil {
		return Config{}, fmt.Errorf("redis url not found")
	}

	mailjetAPIKey, err := extractText("MAILJET_API_KEY")

	if err != nil {
		return Config{}, fmt.Errorf("Mailjet API key not found")
	}

	mailjetSecretKey, err := extractText("MAILJET_SECRET_KEY")

	if err != nil {
		return Config{}, fmt.Errorf("Mailjet secret key not found")
	}

	mailjetFromEmail, err := extractText("MAILJET_FROM_EMAIL")

	if err != nil {
		return Config{}, fmt.Errorf("Mailjet from email not found")
	}

	mailJetFromName, err := extractText("MAILJET_FROM_NAME")

	if err != nil {
		return Config{}, fmt.Errorf("Mailjet from name not found")
	}

	return Config{
		PORT:    port,
		DB_HOST: db_host,
		DB_PASSWORD:      db_pass,
		DB_USER:          db_user,
		DB_NAME:          db_name,
		DB_PORT:          db_port,
		JWTSecret:        jwtSecret,
		CLOUDINARY_URL:   cloudinary,
		REDIS_URL:        redisURL,
		MailjetAPIKey:    mailjetAPIKey,
		MailjetSecretKey: mailjetSecretKey,
		MailjetFromEmail: mailjetFromEmail,
		MailjetFromName:  mailJetFromName,
	}, nil
}

func extractText(key string) (string, error) {

	cf := os.Getenv(key)

	if cf == "" {
		return "", fmt.Errorf("Cant be empty")
	}

	cfg := strings.TrimSpace(cf)

	return cfg, nil
}
