package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"git.dolansoft.org/lorenz/go-zfs/ioctl"
	"github.com/davecgh/go-spew/spew"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

type snapshot struct {
	name string
	ts   time.Time
	used uint64
}

type zfsSnapshotState struct {
	lck sync.Mutex

	snapshotFilter func(string) bool

	// snapshots slice is ordered by timestamp
	datasets map[string][]snapshot

	metricCount        *prometheus.GaugeVec
	metricLastUnixtime *prometheus.GaugeVec
	metricDiskUsed     *prometheus.GaugeVec
}

func newZfsSnapshotState(reg prometheus.Registerer) *zfsSnapshotState {
	return &zfsSnapshotState{
		datasets: make(map[string][]snapshot),

		metricCount: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "zfs",
			Subsystem: "snapshot",
			Name:      "count",
			Help:      "Count of existing ZFS snapshots.",
		}, []string{"dataset"}),
		metricDiskUsed: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "zfs",
			Subsystem: "snapshot",
			Name:      "disk_used",
			Help:      "Disk space used by all snapshots.",
		}, []string{"dataset"}),
		metricLastUnixtime: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "zfs",
			Subsystem: "snapshot",
			Name:      "last_unixtime",
			Help:      "Time of last ZFS snapshot",
		}, []string{"dataset"}),
	}
}

func (z *zfsSnapshotState) Describe(ch chan<- *prometheus.Desc) {
	z.lck.Lock()
	defer z.lck.Unlock()

	z.metricCount.Describe(ch)
	z.metricDiskUsed.Describe(ch)
	z.metricLastUnixtime.Describe(ch)
}

func (z *zfsSnapshotState) Collect(ch chan<- prometheus.Metric) {
	z.lck.Lock()
	defer z.lck.Unlock()

	z.metricCount.Reset()
	z.metricDiskUsed.Reset()
	z.metricLastUnixtime.Reset()

	var (
		used uint64
		last time.Time
	)

	for dataset, snapshots := range z.datasets {
		z.metricCount.WithLabelValues(dataset).Set(float64(len(snapshots)))
		used = 0
		last = time.Time{}
		for _, snap := range snapshots {
			used += snap.used
			last = snap.ts
		}
		z.metricDiskUsed.WithLabelValues(dataset).Set(float64(used))
		z.metricLastUnixtime.WithLabelValues(dataset).Set(float64(last.Unix()))
	}

	z.metricCount.Collect(ch)
	z.metricDiskUsed.Collect(ch)
	z.metricLastUnixtime.Collect(ch)
}

func (z *zfsSnapshotState) add(datasetName string, snapshotName string, size uint64, time time.Time) {
	z.lck.Lock()
	defer z.lck.Unlock()

	snap := snapshot{
		name: snapshotName,
		ts:   time,
		used: size,
	}

	// append to ordered slice, if not existing
	snapshots, ok := z.datasets[datasetName]
	if !ok {
		z.datasets[datasetName] = []snapshot{snap}
		return
	}

	// check if already there, and where it would fit
	idx := 0
	for _, s := range snapshots {
		if s.name == snapshotName {
			return
		}
		if s.ts.After(time) {
			break
		}
		idx++
	}

	// insert at idx
	z.datasets[datasetName] = append(z.datasets[datasetName], snapshot{})
	copy(z.datasets[datasetName][idx+1:], z.datasets[datasetName][idx:])
	z.datasets[datasetName][idx] = snap

}

var (
	nodePath = "/dev/zfs"
	err      error

	logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339Nano}).With().Timestamp().Logger().Level(zerolog.InfoLevel)
)

var ()

func getUsedBytes(prop ioctl.DatasetPropsWithSource) (uint64, bool) {
	v, ok := prop["used"]
	if !ok {
		return 0, false
	}

	vuint, ok := v.Value.(uint64)
	if !ok {
		return 0, false
	}

	return vuint, true
}

func getCreationTime(prop ioctl.DatasetPropsWithSource) (time.Time, bool) {
	v, ok := prop["creation"]
	if !ok {
		return time.Time{}, false
	}

	vint, ok := v.Value.(uint64)
	if !ok {
		return time.Time{}, false
	}

	return time.Unix(int64(vint), 0), true
}

func run(ctx context.Context) func() error {
	return func() error {
		snapshots := newZfsSnapshotState(prometheus.DefaultRegisterer)

		prometheus.MustRegister(snapshots)

		ioctl.Init("")

		var (
			i             uint64
			name          string
			snapshotName  string
			err           error
			props         ioctl.DatasetPropsWithSource
			j             uint64
			stats         ioctl.DMUObjectSetStats
			snapshotProps = make(ioctl.DatasetPropsWithSource)
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

				for {
					snapshotName, j, stats, err = ioctl.SnapshotListNext(name, j, snapshotProps)
					if j == 0 {
						break
					}
					if err != nil {
						return err
					}

					snapshotName = strings.Split(snapshotName, "@")[1]

					logger.Debug().Msgf("snapshot %s (%d) props=%+v", snapshotName, j, snapshotProps)

					used, ok := getUsedBytes(snapshotProps)
					if !ok {
						continue
					}
					creation, ok := getCreationTime(snapshotProps)
					if !ok {
						continue
					}

					snapshots.add(name, snapshotName, used, creation)

					logger.Trace().Msgf("snapshot stats=%s", spew.Sdump(stats))
				}
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
	listenAddr = flag.String("listen-address", ":8080", "The address to listen on for HTTP requests.")
	logLevel   = flag.String("log-level", "info", "Log level")
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	prometheus.MustRegister(collectors.NewBuildInfoCollector())

	flag.Parse()

	// setting log level appropriately
	lvl, err := zerolog.ParseLevel(*logLevel)
	if err != nil {
		logger.Fatal().Msgf("invalid log level: %v", err)
	}
	logger = logger.Level(lvl)

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
