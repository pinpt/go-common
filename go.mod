module github.com/pinpt/go-common

go 1.14

require (
	github.com/bsm/redislock v0.3.0
	github.com/cespare/xxhash v1.1.0
	github.com/confluentinc/confluent-kafka-go v1.4.2 // indirect
	github.com/fatih/color v1.7.0
	github.com/go-kit/kit v0.7.0
	github.com/go-logfmt/logfmt v0.4.0 // indirect
	github.com/go-redis/redis v6.15.6+incompatible
	github.com/go-stack/stack v1.8.0 // indirect
	github.com/google/uuid v1.1.1
	github.com/gorilla/websocket v1.4.1
	github.com/mattn/go-colorable v0.0.9 // indirect
	github.com/mattn/go-isatty v0.0.10
	github.com/oliveagle/jsonpath v0.0.0-20180606110733-2e52cf6e6852
	github.com/pinpt/httpclient v0.0.0-20190815022759-09e0028c9067
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stretchr/testify v1.4.0
	golang.org/x/crypto v0.0.0-20191010185427-af544f31c8ac
	golang.org/x/sys v0.0.0-20191010194322-b09406accb47 // indirect
	gopkg.in/confluentinc/confluent-kafka-go.v1 v1.4.2
	gopkg.in/yaml.v2 v2.2.4 // indirect
)

replace github.com/gorilla/websocket v1.4.1 => github.com/pinpt/websocket v1.4.2-0.20191010233559-d9055c4295fd
