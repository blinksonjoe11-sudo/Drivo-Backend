// package database

// import (
// 	"drivo/internal/config"
// 	"fmt"
// 	"log"
// 	"os"
// 	"strings"
// 	"time"

// 	"gorm.io/driver/postgres"
// 	"gorm.io/gorm"
// 	"gorm.io/gorm/logger"
// )

// func Connect(cfg config.Config) (*gorm.DB, error) {

// 	// connect config and database

// 	dsn := strings.TrimSpace(cfg.DB_HOST)

// 	gormLogger := logger.New(
// 		log.New(os.Stdout, "\r\n", log.LstdFlags),
// 		logger.Config{
// 			SlowThreshold:             500 * time.Millisecond,
// 			LogLevel:                  logger.Warn,
// 			IgnoreRecordNotFoundError: true,
// 			Colorful:                  false,
// 		},
// 	)

// 	db, err := gorm.Open(postgres.New(postgres.Config{
// 		DSN:                  dsn,
// 		PreferSimpleProtocol: true,
// 	}), &gorm.Config{
// 		Logger:                                   gormLogger,
// 		DisableForeignKeyConstraintWhenMigrating: true,
// 		PrepareStmt:                              false,
// 	})
// 	if err != nil {
// 		return nil, fmt.Errorf("error connecting to database: %v", err)
// 	}

// 	sqlDB, err := db.DB()
// 	if err != nil {
// 		return nil, fmt.Errorf("error getting sql.DB: %v", err)
// 	}
// 	sqlDB.SetMaxOpenConns(10)
// 	sqlDB.SetMaxIdleConns(3)
// 	sqlDB.SetConnMaxLifetime(5 * time.Minute)
// 	sqlDB.SetConnMaxIdleTime(1 * time.Minute)

// 	return db, nil
// }

package database

import (
	"drivo/internal/config"
	"fmt"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func Connect(cfg config.Config) (*gorm.DB, error) {

	// connect config and database

	dsn := fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%s sslmode=disable",
		cfg.DB_HOST,
		cfg.DB_USER,
		cfg.DB_PASSWORD,
		cfg.DB_NAME,
		cfg.DB_PORT,
	)

	dbc, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("error connecting to db")
	}

	return dbc, nil
}
