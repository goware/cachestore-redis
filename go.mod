module github.com/goware/cachestore-redis

go 1.23.0

replace github.com/goware/cachestore2 => ../cachestore2

require (
	github.com/goware/cachestore2 v0.0.0-00010101000000-000000000000
	github.com/redis/go-redis/v9 v9.7.3
	github.com/stretchr/testify v1.9.0
	golang.org/x/sync v0.12.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
