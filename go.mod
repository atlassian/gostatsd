module github.com/atlassian/gostatsd

go 1.13

require (
	github.com/ash2k/stager v0.0.0-20170622123058-6e9c7b0eacd4
	github.com/aws/aws-sdk-go v1.28.6
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/dvyukov/go-fuzz v0.0.0-20191206100749-a378175e205c
	github.com/elazarl/go-bindata-assetfs v1.0.0 // indirect
	github.com/go-redis/redis v6.15.6+incompatible
	github.com/golang/protobuf v1.3.2
	// # Unpin github.com/golangci/golangci-lint from v1.18.0 when gitea 1.10 is released
	// # https://github.com/go-gitea/gitea/issues/8126
	// # https://github.com/atlassian/gostatsd/issues/266
	github.com/golangci/golangci-lint v1.18.0
	github.com/gorilla/mux v1.7.3
	github.com/googleapis/gnostic v0.3.1 // indirect
	github.com/howeyc/fsnotify v0.9.0 // indirect
	github.com/json-iterator/go v1.1.7
	github.com/jstemmer/go-junit-report v0.9.1 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/libp2p/go-reuseport v0.0.1
	github.com/magiconair/properties v1.8.1
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/pelletier/go-toml v1.6.0 // indirect
	github.com/mozilla/tls-observatory v0.0.0-20190404164649-a3c1b6cfecfd // indirect
	github.com/pkg/errors v0.8.1
	github.com/sirupsen/logrus v1.4.2
	github.com/spf13/cast v1.3.1 // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.6.2
	github.com/stephens2424/writerset v1.0.2 // indirect
	github.com/stretchr/testify v1.4.0
	github.com/tilinna/clock v1.0.2
	golang.org/x/net v0.0.0-20200114155413-6afb5195e5aa
	golang.org/x/sys v0.0.0-20200120151820-655fe14d7479 // indirect
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
	golang.org/x/tools v0.0.0-20200119215504-eb0d8dd85bcc
	gopkg.in/ini.v1 v1.51.1 // indirect
	golang.org/x/net v0.0.0-20190916140828-c8589233b77d
	golang.org/x/tools v0.0.0-20190917215024-905c8ffbfa41 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	k8s.io/api v0.0.0-20191004102349-159aefb8556b
	k8s.io/apimachinery v0.0.0-20191004074956-c5d2f014d689
	k8s.io/client-go v11.0.1-0.20191029005444-8e4128053008+incompatible
	k8s.io/kube-openapi v0.0.0-20200204173128-addea2498afe // indirect
	k8s.io/utils v0.0.0-20200117235808-5f6fbceb4c31 // indirect
)

go 1.13
