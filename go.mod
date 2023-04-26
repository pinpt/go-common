module github.com/pinpt/go-common/v10

go 1.14

require (
	github.com/cespare/xxhash v1.1.0
	github.com/fatih/color v1.9.0
	github.com/go-kit/kit v0.10.0
	github.com/google/uuid v1.1.1
	github.com/gorilla/websocket v1.4.2
	github.com/mattn/go-colorable v0.1.6 // indirect
	github.com/mattn/go-isatty v0.0.12
	github.com/nlopes/slack v0.6.0
	github.com/oliveagle/jsonpath v0.0.0-20180606110733-2e52cf6e6852
	github.com/pinpt/httpclient v0.0.0-20190815022759-09e0028c9067
	github.com/prometheus/client_golang v1.11.1
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/streadway/amqp v0.0.0-20200108173154-1c71cc93ed71
	github.com/stretchr/testify v1.6.0
	golang.org/x/crypto v0.1.0
)

replace github.com/gorilla/websocket v1.4.2 => github.com/pinpt/websocket v1.4.2-0.20191010233559-d9055c4295fd
