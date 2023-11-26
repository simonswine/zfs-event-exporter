package snapshot

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

func cmdListSnapshots(ctx context.Context, args ...string) ([]byte, error) {
	args = append([]string{"list", "-H", "-p", "-t", "snapshot", "-o", "name,creation,used"}, args...)
	return exec.Command("zfs", args...).Output()
}

func cmdZpoolEvents(ctx context.Context, out io.Writer) error {
	cmd := exec.CommandContext(ctx,
		"zpool",
		"events",
		"-f",
		"-H",
		"-v",
	)
	cmd.Stdout = out
	return cmd.Start()
}

type snapshotState struct {
	name string
	ts   time.Time
	used uint64
}

type snapshotCollector struct {
	lck    sync.Mutex
	logger zerolog.Logger

	datasets      snapshotsState
	listSnapshots func(context.Context, ...string) ([]byte, error)

	metricCount        *prometheus.GaugeVec
	metricLastUnixtime *prometheus.GaugeVec
	metricDiskUsed     *prometheus.GaugeVec
}

func NewCollector(ctx context.Context, logger zerolog.Logger) (*snapshotCollector, error) {
	var (
		eventCh                  = make(chan *zpoolEvent)
		eventReader, eventWriter = io.Pipe()
	)

	if err := cmdZpoolEvents(ctx, eventWriter); err != nil {
		return nil, fmt.Errorf("failed to start zpool events: %w", err)
	}

	go func() {
		if err := parseZpoolEvents(eventReader, eventCh); err != nil {
			logger.Error().Err(err).Msg("failed to parse zpool events")
		}
	}()

	return newCollector(ctx, logger, cmdListSnapshots, eventCh)

}

type snapshotsState map[string][]snapshotState

func (s snapshotsState) parse(r io.Reader) error {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return fmt.Errorf("invalid line: %q", line)
		}

		idx := strings.LastIndex(fields[0], "@")
		if idx == -1 {
			return fmt.Errorf("invalid snapshot name: %q", fields[0])
		}

		tsUnix, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid timestamp: %q", fields[1])
		}
		ts := time.Unix(tsUnix, 0)

		used, err := strconv.ParseUint(fields[2], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid used bytes: %q", fields[2])
		}

		dataset := fields[0][:idx]

		s[dataset] = append(s[dataset], snapshotState{
			name: fields[0][idx+1:],
			ts:   ts,
			used: used,
		})
	}

	for _, snapshots := range s {
		sort.Slice(snapshots, func(i, j int) bool {
			return snapshots[i].ts.Before(snapshots[j].ts)
		})
	}

	return nil
}

func newCollector(ctx context.Context, logger zerolog.Logger, listSnapshots func(context.Context, ...string) ([]byte, error), eventCh chan *zpoolEvent) (*snapshotCollector, error) {
	data, err := listSnapshots(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}

	datasets := make(snapshotsState)

	if err := datasets.parse(bytes.NewReader(data)); err != nil {
		return nil, fmt.Errorf("failed to parse snapshots: %w", err)
	}

	c := &snapshotCollector{
		logger:        logger.With().Str("collector", "snapshot").Logger(),
		datasets:      datasets,
		listSnapshots: listSnapshots,
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

	go func() {
		err := c.eventLoop(ctx, eventCh)
		if err != nil {
			c.logger.Error().Err(err).Msg("snapshot event loop failed")
		}
	}()

	return c, nil
}

func (c *snapshotCollector) removeSnapshot(datasetName string, snapshotName string) {
	c.lck.Lock()
	defer c.lck.Unlock()

	snapshots, ok := c.datasets[datasetName]
	if !ok {
		return
	}

	for i, snap := range snapshots {
		if snap.name == snapshotName {
			// remove snapshot
			c.datasets[datasetName] = append(c.datasets[datasetName][:i], c.datasets[datasetName][i+1:]...)
		}
	}
}

func (c *snapshotCollector) addSnapshot(datasetName string, snapshotName string) error {
	data, err := c.listSnapshots(context.Background(), datasetName)
	if err != nil {
		return err
	}

	c.lck.Lock()
	defer c.lck.Unlock()

	return c.datasets.parse(bytes.NewReader(data))
}

func (c *snapshotCollector) eventLoop(ctx context.Context, eventCh chan *zpoolEvent) error {
	if eventCh == nil {
		return nil
	}
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case event := <-eventCh:
			if event.HistoryInternalName != "snapshot" && event.HistoryInternalName != "destroy" {
				continue
			}

			idx := strings.LastIndex(event.HistoryDSName, "@")
			if idx == -1 {
				continue
			}

			dataset := event.HistoryDSName[:idx]
			snapshot := event.HistoryDSName[idx+1:]

			if event.HistoryInternalName == "destroy" {
				c.removeSnapshot(dataset, snapshot)
				continue
			}

			if err := c.addSnapshot(dataset, snapshot); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *snapshotCollector) Describe(ch chan<- *prometheus.Desc) {
	c.metricCount.Describe(ch)
	c.metricDiskUsed.Describe(ch)
	c.metricLastUnixtime.Describe(ch)
}

func (c *snapshotCollector) Collect(ch chan<- prometheus.Metric) {
	c.lck.Lock()
	defer c.lck.Unlock()

	c.metricCount.Reset()
	c.metricDiskUsed.Reset()
	c.metricLastUnixtime.Reset()

	var (
		used uint64
		last time.Time
	)

	for dataset, snapshots := range c.datasets {
		c.metricCount.WithLabelValues(dataset).Set(float64(len(snapshots)))
		used = 0
		last = time.Time{}
		for _, snap := range snapshots {
			used += snap.used
			last = snap.ts
		}
		c.metricDiskUsed.WithLabelValues(dataset).Set(float64(used))
		c.metricLastUnixtime.WithLabelValues(dataset).Set(float64(last.Unix()))
	}

	c.metricCount.Collect(ch)
	c.metricDiskUsed.Collect(ch)
	c.metricLastUnixtime.Collect(ch)
}

type zpoolEvent struct {
	HistoryInternalName string
	HistoryDSName       string
	Time                time.Time
}

func trimDoubleQuotes(s string) string {
	if len(s) < 2 {
		return s
	}

	if s[0] != '"' || s[len(s)-1] != '"' {
		return s
	}

	return s[1 : len(s)-1]
}

func parseZpoolEvents(r io.Reader, ch chan *zpoolEvent) error {
	var (
		scanner = bufio.NewScanner(r)
		lineno  = -1
		event   = new(zpoolEvent)
	)
	for scanner.Scan() {
		lineno++
		line := scanner.Text()
		if line == "" {
			ch <- event
			event = new(zpoolEvent)
			lineno = -1
			continue
		}
		if lineno == 0 {
			continue
		}
		// find the separator between the key and the value
		sep := strings.IndexByte(line, '=')
		if sep < 1 {
			continue
		}
		if len(line) < sep+2 {
			continue
		}
		key := strings.TrimSpace(line[:sep-1])
		value := line[sep+2:]

		switch key {
		case "time":
			fields := strings.Fields(value)
			if len(fields) >= 2 {
				secs, err := strconv.ParseInt(fields[0], 0, 64)
				if err != nil {
					return fmt.Errorf("unable to parse seconds: %w", err)
				}
				nanos, err := strconv.ParseInt(fields[1], 0, 64)
				if err != nil {
					return fmt.Errorf("unable to parse nano seconds: %w", err)
				}
				event.Time = time.Unix(secs, nanos)
			}
		case "history_internal_name":
			event.HistoryInternalName = trimDoubleQuotes(value)
		case "history_dsname":
			event.HistoryDSName = trimDoubleQuotes(value)
		default:
			break
		}
	}
	if scanner.Err() != nil {
		return fmt.Errorf("scanner error: %w", scanner.Err())
	}

	return nil
}
