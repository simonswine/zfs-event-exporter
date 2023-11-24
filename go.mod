module github.com/simonswine/zfs-event-listener

go 1.20

require (
	github.com/davecgh/go-spew v1.1.1
	github.com/prometheus/client_golang v1.12.2
	github.com/rs/zerolog v1.31.0
	golang.org/x/sync v0.0.0-20201207232520-09787c993a3a
)

require (
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
)

require (
	git.dolansoft.org/lorenz/go-zfs v0.0.0-20211205151927-3df88f338885
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.32.1 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	golang.org/x/sys v0.12.0 // indirect
	google.golang.org/protobuf v1.26.0 // indirect
)

replace git.dolansoft.org/lorenz/go-zfs => github.com/lorenz/go-zfs v0.0.0-20230225101216-f5e946970e2d
