module github.com/pingcap/go-ycsb

require (
	cloud.google.com/go/spanner v1.0.0
	cloud.google.com/go/storage v1.0.0 // indirect
	github.com/XiaoMi/pegasus-go-client v0.0.0-20181029071519-9400942c5d1c
	github.com/aerospike/aerospike-client-go v1.35.2
	github.com/apple/foundationdb v0.0.0-20181113211710-8903c5f6212a
	github.com/bitly/go-hostpool v0.0.0-20171023180738-a3a6125de932 // indirect
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/boltdb/bolt v1.3.1
	github.com/chzyer/logex v1.1.10 // indirect
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/chzyer/test v0.0.0-20180213035817-a1ea475d72b1 // indirect
	github.com/dgraph-io/badger v1.6.0
	github.com/dustin/go-humanize v1.0.0
	github.com/fortytw2/leaktest v1.3.0 // indirect
	github.com/ghodss/yaml v1.0.1-0.20190212211648-25d852aebe32 // indirect
	github.com/go-ini/ini v1.49.0 // indirect
	github.com/go-redis/redis v6.15.1+incompatible
	github.com/go-sql-driver/mysql v1.4.1
	github.com/gocql/gocql v0.0.0-20181124151448-70385f88b28b
	github.com/google/go-cmp v0.3.1 // indirect
	github.com/google/pprof v0.0.0-20190908185732-236ed259b199 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.1-0.20190118093823-f849b5445de4 // indirect
	github.com/hashicorp/golang-lru v0.5.3 // indirect
	github.com/kr/pty v1.1.8 // indirect
	github.com/lib/pq v1.0.0
	github.com/magiconair/properties v1.8.0
	github.com/mattn/go-sqlite3 v1.10.0
	github.com/minio/minio v0.0.0-20190903181048-8a71b0ec5a72
	github.com/minio/minio-go v6.0.14+incompatible
	github.com/minio/minio-go/v6 v6.0.39 // indirect
	github.com/pingcap/errors v0.11.1
	github.com/pingcap/kvproto v0.0.0-20190506024016-26344dff8f48 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20190512091148-babf20351dd7 // indirect
	github.com/rogpeppe/go-internal v1.3.2 // indirect
	github.com/spf13/cobra v0.0.5
	github.com/tecbot/gorocksdb v0.0.0-20190519120508-025c3cf4ffb4
	github.com/tidwall/pretty v1.0.0 // indirect
	github.com/tikv/client-go v0.0.0-20190421092910-44b82dcc9f4a
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c // indirect
	github.com/xdg/stringprep v1.0.0 // indirect
	github.com/yuin/gopher-lua v0.0.0-20181031023651-12c4817b42c5 // indirect
	go.mongodb.org/mongo-driver v1.0.2
	go.opencensus.io v0.22.1 // indirect
	golang.org/x/exp v0.0.0-20190912063710-ac5d2bfcbfe0 // indirect
	golang.org/x/image v0.0.0-20190910094157-69e4b8554b2a // indirect
	golang.org/x/mobile v0.0.0-20190910184405-b558ed863381 // indirect
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e // indirect
	google.golang.org/api v0.10.0
	google.golang.org/appengine v1.6.2 // indirect
	google.golang.org/genproto v0.0.0-20190916214212-f660b8655731
	google.golang.org/grpc v1.23.1 // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/tomb.v2 v2.0.0-20161208151619-d5d1b5820637 // indirect
)

replace github.com/apache/thrift => github.com/apache/thrift v0.0.0-20171203172758-327ebb6c2b6d

// replace github.com/tecbot/gorocksdb => github.com/DorianZheng/gorocksdb v0.0.0-20180811132858-ab9bf2cc4b67

replace github.com/minio/minio => /root/workspace/go/src/github.com/minio/minio

go 1.13
