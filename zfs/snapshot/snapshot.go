package pool

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
)

func cmdListSnapshots(ctx context.Context) ([]byte, error) {
	return exec.Command("zfs", "list", "-H", "-p", "-t", "snapshot", "-o", "name,creation,used").Output()
}

type snapshotState struct {
	name string
	ts   time.Time
	used uint64
}

type snapshotCollector struct {
	lck sync.Mutex

	datasets map[string][]snapshotState

	metricCount        *prometheus.GaugeVec
	metricLastUnixtime *prometheus.GaugeVec
	metricDiskUsed     *prometheus.GaugeVec
}

func NewCollector(ctx context.Context) (*snapshotCollector, error) {
	return newCollector(ctx, cmdListSnapshots)
}

func parseSnapshots(r io.Reader) (map[string][]snapshotState, error) {
	datasets := make(map[string][]snapshotState)

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return nil, fmt.Errorf("invalid line: %q", line)
		}

		idx := strings.LastIndex(fields[0], "@")
		if idx == -1 {
			return nil, fmt.Errorf("invalid snapshot name: %q", fields[0])
		}

		tsUnix, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid timestamp: %q", fields[1])
		}
		ts := time.Unix(tsUnix, 0)

		used, err := strconv.ParseUint(fields[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid used bytes: %q", fields[2])
		}

		dataset := fields[0][:idx]

		datasets[dataset] = append(datasets[dataset], snapshotState{
			name: fields[0][idx+1:],
			ts:   ts,
			used: used,
		})
	}

	for _, snapshots := range datasets {
		sort.Slice(snapshots, func(i, j int) bool {
			return snapshots[i].ts.Before(snapshots[j].ts)
		})
	}

	return datasets, nil
}

func newCollector(ctx context.Context, listSnapshots func(context.Context) ([]byte, error)) (*snapshotCollector, error) {

	data, err := listSnapshots(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}

	datasets, err := parseSnapshots(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to parse snapshots: %w", err)
	}

	return &snapshotCollector{
		datasets: datasets,
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
	}, nil
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
			fmt.Println("event end")
			ch <- event
			event = new(zpoolEvent)
			lineno = -1
			continue
		}
		if lineno == 0 {
			fmt.Println("event start", line)
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
			fmt.Printf("unused key=%s value=%s\n", key, value)
		}
	}
	if scanner.Err() != nil {
		return fmt.Errorf("scanner error: %w", scanner.Err())
	}

	return nil
}
