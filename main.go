package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"git.dolansoft.org/lorenz/go-zfs/ioctl"
	"github.com/davecgh/go-spew/spew"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

type datasetSnapshotState struct {
	snapshots map[string]time.Time
}

type zfsSnapshotState struct {
	datasets map[string]datasetSnapshotState
}

var (
	nodePath = "/dev/zfs"
	err      error

	logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339Nano}).With().Timestamp().Logger().Level(zerolog.DebugLevel)
)

var (
	snapshotCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "zfs",
		Subsystem: "snapshot",
		Name:      "count",
		Help:      "Snapshots per dataset",
	}, []string{"dataset"})
	lastSnapshot = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "zfs",
		Subsystem: "snapshot",
		Name:      "last_created_unix",
		Help:      "Unixtime of last snapshot created",
	}, []string{"dataset"})
)

func run(ctx context.Context) func() error {
	return func() error {
		ioctl.Init("")

		var (
			i               uint64
			name            string
			snapshotName    string
			err             error
			props           ioctl.DatasetPropsWithSource
			j               uint64
			dsSnapshotCount uint64
			dsLastSnapshot  time.Time
			stats           ioctl.DMUObjectSetStats
			snapshotProps   = make(ioctl.DatasetPropsWithSource)
		)

		// get dataset list
		datasets, err := getPools()
		if err != nil {
			return err
		}

		for {
			if len(datasets) == 0 {
				break
			}
			dataset := datasets[0]
			datasets = datasets[1:]
			i = 0
			for {
				name, i, _, props, err = ioctl.DatasetListNext(dataset, i)
				if i == 0 {
					break
				}
				if err != nil {
					return err
				}
				logger.Debug().Msgf("dataset %s (%d)", name, i)
				logger.Trace().Msgf("dataset props %v", props)

				// look into child datasets
				datasets = append(datasets, name)

				j = 0
				dsSnapshotCount = 0
				dsLastSnapshot = time.Time{}

				for {
					snapshotName, j, stats, err = ioctl.SnapshotListNext(name, j, snapshotProps)
					if j == 0 {
						break
					}
					if err != nil {
						return err
					}

					snapshotName = strings.Split(snapshotName, "@")[1]
					ts, err := time.Parse(*snapshotTemplate, snapshotName)
					if err != nil {
						logger.Warn().Msgf("failed to parse timestamp from snapshot name %s: %v", snapshotName, err)
						continue
					}

					if ts.After(dsLastSnapshot) {
						dsLastSnapshot = ts
					}

					logger.Debug().Msgf("snapshot %s (%d) props=%+v", snapshotName, j, snapshotProps)
					logger.Trace().Msgf("snapshot stats=%s", spew.Sdump(stats))
					dsSnapshotCount++
				}
				snapshotCount.WithLabelValues(name).Set(float64(dsSnapshotCount))
				lastSnapshot.WithLabelValues(name).Set(float64(dsLastSnapshot.Unix()))

			}
		}
		return nil
	}
}

func getPools() ([]string, error) {
	ioctl.Init("")
	var poolNames []string
	poolConfigs, err := ioctl.PoolConfigs()
	if err != nil {
		return nil, err
	}

	for poolName, cfg := range poolConfigs {
		pCfg, ok := cfg.(map[string]interface{})
		if !ok {
			continue
		}

		logger.Debug().Msgf("found pool %+v", poolName)
		logger.Trace().Msgf("pool cfg %+v", pCfg)

		stats, err := ioctl.PoolStats(poolName)
		if err != nil {
			return nil, err
		}
		logger.Trace().Msgf("pool stats %+v", stats)

		poolNames = append(poolNames, poolName)
	}

	return poolNames, nil
}

func watchEvents() error {

	zfsHandle, err := os.OpenFile(nodePath, os.O_RDWR|os.O_EXCL, 0)
	if err != nil {
		return fmt.Errorf("Failed to open or create ZFS device node: %v", err)
	}

	eventHandle, err := os.OpenFile(nodePath, os.O_RDWR, 0)
	if err != nil {
		return err
	}

	i := 0

	for {
		cmd := &ioctl.Cmd{
			Cleanup_fd: int32(eventHandle.Fd()),
			Cookie:     0,
		}
		res := make(map[string]interface{})
		if err := ioctl.NvlistIoctl(zfsHandle.Fd(), ioctl.ZFS_IOC_EVENTS_NEXT, "", cmd, nil, res, nil); err != nil {
			return err
		}
		logger.Printf("found history %+v", res)
		i++
		if i > 100000 {
			break
		}
	}

	return nil
}

var (
	listenAddr       = flag.String("listen-address", ":8080", "The address to listen on for HTTP requests.")
	snapshotTemplate = flag.String("snapshot-template", "zrepl_20060102_150405_000", "The template for reading timestamps from snapshot names")
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	prometheus.MustRegister(collectors.NewBuildInfoCollector())

	flag.Parse()

	g, ctx := errgroup.WithContext(ctx)

	g.Go(run(ctx))

	/*

		go func() {
			if err := watchEvents(); err != nil {
				logger.Fatal(err)
			}
		}()
	*/

	srv := &http.Server{Addr: *listenAddr}
	mux := http.NewServeMux()
	srv.Handler = mux

	// Expose the registered metrics via HTTP.
	mux.Handle("/metrics", promhttp.HandlerFor(
		prometheus.DefaultGatherer,
		promhttp.HandlerOpts{
			// Opt into OpenMetrics to support exemplars.
			EnableOpenMetrics: true,
		},
	))

	go func() {
		<-ctx.Done()
		logger.Debug().Msg("shutting down http server")
		srv.Shutdown(context.Background())
	}()

	g.Go(srv.ListenAndServe)

	if err := g.Wait(); err != nil {
		logger.Fatal().Msgf("error running: %v", err)
	}
}
