package pool

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

var (
	poolStates = []string{
		"online",
		"degraded",
		"faulted",
		"offline",
		"unavail",
		"removed",
	}
)

func zpoolStatusCmd() ([]byte, error) {
	return exec.Command("zpool", "status", "-pP").Output()
}

func setStatus(m *prometheus.GaugeVec, labelValues ...string) {
	if len(labelValues) < 2 {
		panic("invalid labelValues")
	}
	status := strings.ToLower(labelValues[len(labelValues)-1])

	for _, s := range poolStates {
		value := 0.0
		if s == status {
			value = 1.0
		}
		labelValues[len(labelValues)-1] = s
		m.WithLabelValues(labelValues...).Set(value)
	}
}

type poolCollector struct {
	logger zerolog.Logger

	metricStatus     *prometheus.GaugeVec
	metricErrors     *prometheus.CounterVec
	metricDiskStatus *prometheus.GaugeVec
	metricDiskErrors *prometheus.CounterVec

	getStatus func() ([]byte, error)
}

func NewCollector(logger zerolog.Logger) *poolCollector {
	return &poolCollector{
		logger: logger.With().Str("collector", "pool").Logger(),

		getStatus: zpoolStatusCmd,

		metricStatus: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "zfs_pool_status",
				Help: "Status of ZFS pool",
			},
			[]string{"pool", "state"},
		),
		metricErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "zfs_pool_errors_total",
				Help: "Total count of ZFS pool errors",
			},
			[]string{"pool", "type"},
		),
		metricDiskStatus: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "zfs_pool_disk_status",
				Help: "Status of a single disk in a ZFS pool",
			},
			[]string{"disk", "pool", "state"},
		),
		metricDiskErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "zfs_pool_disk_errors_total",
				Help: "Total count of ZFS disk errors",
			},
			[]string{"disk", "pool", "type"},
		),
	}
}

type zpoolErrors struct {
	Cksum uint64
	Read  uint64
	Write uint64
}

func (e *zpoolErrors) setErrors(m *prometheus.CounterVec, labelValues ...string) {
	if e == nil {
		return
	}
	m.WithLabelValues(append(labelValues, "read")...).Add(float64(e.Read))
	m.WithLabelValues(append(labelValues, "write")...).Add(float64(e.Write))
	m.WithLabelValues(append(labelValues, "checksum")...).Add(float64(e.Cksum))
}

type poolStatus struct {
	Name   string
	Health string
	Errors *zpoolErrors
}

type diskStatus struct {
	poolStatus
	Pool string
}

type zpoolStatus struct {
	pools []*poolStatus
	disks []*diskStatus
}

func parseErrors(fields []string) (*zpoolErrors, error) {
	if len(fields) < 5 {
		return nil, fmt.Errorf("not enough fields in output")
	}

	var merr error

	read, err := strconv.ParseUint(fields[2], 10, 64)
	if err != nil {
		merr = errors.Join(merr, fmt.Errorf("error parsing read errors: %w", err))
	}
	write, err := strconv.ParseUint(fields[3], 10, 64)
	if err != nil {
		merr = errors.Join(merr, fmt.Errorf("error parsing write errors: %w", err))
	}
	cksum, err := strconv.ParseUint(fields[4], 10, 64)
	if err != nil {
		merr = errors.Join(merr, fmt.Errorf("error parsing cksum errors: %w", err))
	}

	if merr != nil {
		return nil, merr
	}

	return &zpoolErrors{
		Read:  read,
		Write: write,
		Cksum: cksum,
	}, nil
}

type zpoolConfigLine string

func (z zpoolConfigLine) Fields() []string {
	return strings.Fields(string(z))
}

func (z zpoolConfigLine) Level() int {
	level := 0
	for _, c := range string(z) {
		if c == '\t' {
			continue
		}
		if c == ' ' {
			level++
		} else {
			break
		}
	}
	return level / 2
}

type poolTrace []string

func (p poolTrace) Pool() string {
	off := p
	if len(off) >= 2 && off[0] == off[1] {
		off = off[1:]
	}

	if off.Disk() != "" {
		off = off[:len(off)-1]
	}

	return strings.Join(off, "/")
}

func (p poolTrace) Disk() string {
	if len(p) >= 2 && strings.HasPrefix(p[len(p)-1], "/") {
		return p[len(p)-1]
	}
	return ""
}

func parseStatus(r io.Reader) (*zpoolStatus, error) {

	var (
		result         = new(zpoolStatus)
		diskLineOffset int
		trace          poolTrace
	)

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := zpoolConfigLine(scanner.Text())
		fields := line.Fields()
		if len(fields) < 1 {
			continue
		}
		if fields[0] == "pool:" {
			diskLineOffset = -1
			trace = []string{fields[1]}
		}
		if fields[0][len(fields[0])-1] != ':' {
			if fields[0] == "NAME" {
				if offset := strings.Index(string(line), "NAME"); offset > 0 {
					diskLineOffset = offset
				}
			} else if diskLineOffset >= 0 {
				// remove whitespaces before the disk name
				line = line[diskLineOffset:]

				// add the disk name to the trace (at the right level), to respect the hierarchy.
				trace = trace[0 : line.Level()+1]
				trace = append(trace, fields[0])

				// line doesn't contain error counts
				if len(fields) < 5 {
					continue
				}

				e, err := parseErrors(fields)
				if err != nil {
					return nil, err
				}

				if disk := trace.Disk(); disk != "" {
					// we are a disk
					result.disks = append(result.disks, &diskStatus{
						Pool: trace.Pool(),
						poolStatus: poolStatus{
							Name:   disk,
							Health: fields[1],
							Errors: e,
						},
					})
				} else {
					// we are a pool
					result.pools = append(result.pools, &poolStatus{
						Name:   trace.Pool(),
						Health: fields[1],
						Errors: e,
					})
				}
			}
		}
	}

	return result, nil
}

func (pc *poolCollector) Collect(ch chan<- prometheus.Metric) {
	data, err := pc.getStatus()
	if err != nil {
		panic(err)
	}

	zpools, err := parseStatus(bytes.NewReader(data))
	if err != nil {
		panic(err)
	}

	pc.metricStatus.Reset()
	pc.metricErrors.Reset()
	pc.metricDiskStatus.Reset()
	pc.metricDiskErrors.Reset()

	for _, zpool := range zpools.pools {
		setStatus(pc.metricStatus, zpool.Name, zpool.Health)
		zpool.Errors.setErrors(pc.metricErrors, zpool.Name)
	}
	for _, disk := range zpools.disks {
		setStatus(pc.metricDiskStatus, disk.Name, disk.Pool, disk.Health)
		disk.Errors.setErrors(pc.metricDiskErrors, disk.Name, disk.Pool)
	}

	if err != nil {
		fmt.Println(err)
		return
	}
	pc.metricStatus.Collect(ch)
	pc.metricErrors.Collect(ch)
	pc.metricDiskStatus.Collect(ch)
	pc.metricDiskErrors.Collect(ch)
}

func (pc *poolCollector) Describe(ch chan<- *prometheus.Desc) {
	pc.metricStatus.Describe(ch)
	pc.metricErrors.Describe(ch)
	pc.metricDiskStatus.Describe(ch)
	pc.metricDiskErrors.Describe(ch)
}
