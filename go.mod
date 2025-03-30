module github.com/goware/cachestore-redis

go 1.23.2

replace github.com/goware/cachestore => ../cachestore

require (
	github.com/goware/cachestore v0.10.0
	github.com/redis/go-redis/v9 v9.6.1
	github.com/stretchr/testify v1.9.0
	golang.org/x/sync v0.8.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
