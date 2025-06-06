package rediscache

import (
	"time"

	cachestore "github.com/goware/cachestore2"
)

type Config struct {
	cachestore.StoreOptions
	Enabled   bool          `toml:"enabled"`
	Host      string        `toml:"host"`
	Port      uint16        `toml:"port"`
	DBIndex   int           `toml:"db_index"`   // default 0
	MaxIdle   int           `toml:"max_idle"`   // default 4
	MaxActive int           `toml:"max_active"` // default 8
	KeyTTL    time.Duration `toml:"key_ttl"`    // default 1 day
}
