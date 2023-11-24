package pool

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestPoolMetrics(t *testing.T) {
	for _, tc := range []struct {
		name string

		expectedMetrics string
	}{
		{
			name: "simple",
			expectedMetrics: `
# HELP zfs_snapshot_count Count of existing ZFS snapshots.
# TYPE zfs_snapshot_count gauge
zfs_snapshot_count{dataset="pool-hdd/backup/pull/node-a/data"} 2
zfs_snapshot_count{dataset="pool-nvme/data"} 2
# HELP zfs_snapshot_disk_used Disk space used by all snapshots.
# TYPE zfs_snapshot_disk_used gauge
zfs_snapshot_disk_used{dataset="pool-hdd/backup/pull/node-a/data"} 24772608
zfs_snapshot_disk_used{dataset="pool-nvme/data"} 3571712
# HELP zfs_snapshot_last_unixtime Time of last ZFS snapshot
# TYPE zfs_snapshot_last_unixtime gauge
zfs_snapshot_last_unixtime{dataset="pool-hdd/backup/pull/node-a/data"} 1667320886
zfs_snapshot_last_unixtime{dataset="pool-nvme/data"} 1602276642
			`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("testdata", "snapshots-"+tc.name+".txt"))
			require.NoError(t, err)

			ctx := context.Background()
			reg := prometheus.NewPedanticRegistry()
			c, err := newCollector(ctx, func(context.Context) ([]byte, error) {
				return data, nil
			})
			require.NoError(t, err)
			reg.MustRegister(c)

			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(tc.expectedMetrics)))
			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(tc.expectedMetrics)))
		})
	}
}

func TestZpoolEvents(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "events-simple.txt"))
	require.NoError(t, err)

	var (
		ch     = make(chan *zpoolEvent)
		done   = make(chan struct{})
		events []*zpoolEvent
	)

	go func() {
		for e := range ch {
			events = append(events, e)
		}
		close(done)
	}()

	require.NoError(t, parseZpoolEvents(bytes.NewReader(data), ch))
	close(ch)

	<-done

	result, err := json.Marshal(events)
	require.NoError(t, err)

	require.JSONEq(t, `
[
    {
        "HistoryInternalName": "destroy",
        "HistoryDSName": "pool-hdd/backup/data0/%recv",
        "Time": "2023-11-23T03:45:50.763089998Z"
    },
    {
        "HistoryInternalName": "hold",
        "HistoryDSName": "pool-hdd/backup/data0@zrepl_20231122_230701_000",
        "Time": "2023-11-23T03:45:51.005089471Z"
    },
    {
        "HistoryInternalName": "release",
        "HistoryDSName": "pool-hdd/backup/data0@zrepl_20231122_225701_000",
        "Time": "2023-11-23T03:45:51.210089024Z"
    },
    {
        "HistoryInternalName": "receive",
        "HistoryDSName": "pool-hdd/backup/var/%recv",
        "Time": "2023-11-23T03:45:52.374086487Z"
    },
    {
        "HistoryInternalName": "finish receiving",
        "HistoryDSName": "pool-hdd/backup/var/%recv",
        "Time": "2023-11-23T03:45:52.591086014Z"
    },
    {
        "HistoryInternalName": "clone swap",
        "HistoryDSName": "pool-hdd/backup/var/%recv",
        "Time": "2023-11-23T03:45:52.592086012Z"
    },
    {
        "HistoryInternalName": "snapshot",
        "HistoryDSName": "pool-hdd/backup/var@zrepl_20231122_231701_000",
        "Time": "2023-11-23T03:45:52.59308601Z"
    },
    {
        "HistoryInternalName": "destroy",
        "HistoryDSName": "pool-hdd/backup/var/%recv",
        "Time": "2023-11-23T03:45:52.596086004Z"
    },
    {
        "HistoryInternalName": "hold",
        "HistoryDSName": "pool-hdd/backup/var@zrepl_20231122_231701_000",
        "Time": "2023-11-23T03:45:52.819085518Z"
    },
    {
        "HistoryInternalName": "release",
        "HistoryDSName": "pool-hdd/backup/var@zrepl_20231122_230701_000",
        "Time": "2023-11-23T03:45:52.999085125Z"
    },
    {
        "HistoryInternalName": "receive",
        "HistoryDSName": "pool-hdd/backup/data0/%recv",
        "Time": "2023-11-23T03:45:54.156082603Z"
    },
    {
        "HistoryInternalName": "finish receiving",
        "HistoryDSName": "pool-hdd/backup/data0/%recv",
        "Time": "2023-11-23T03:45:54.480081897Z"
    },
    {
        "HistoryInternalName": "clone swap",
        "HistoryDSName": "pool-hdd/backup/data0/%recv",
        "Time": "2023-11-23T03:45:54.481081895Z"
    },
    {
        "HistoryInternalName": "snapshot",
        "HistoryDSName": "pool-hdd/backup/data0@zrepl_20231122_231701_000",
        "Time": "2023-11-23T03:45:54.482081893Z"
    },
    {
        "HistoryInternalName": "destroy",
        "HistoryDSName": "pool-hdd/backup/data0/%recv",
        "Time": "2023-11-23T03:45:54.486081884Z"
    },
    {
        "HistoryInternalName": "hold",
        "HistoryDSName": "pool-hdd/backup/data0@zrepl_20231122_231701_000",
        "Time": "2023-11-23T03:45:54.801081197Z"
    },
    {
        "HistoryInternalName": "release",
        "HistoryDSName": "pool-hdd/backup/data0@zrepl_20231122_230701_000",
        "Time": "2023-11-23T03:45:54.976080816Z"
    },
    {
        "HistoryInternalName": "destroy",
        "HistoryDSName": "pool-hdd/backup/var@zrepl_20231120_095659_000",
        "Time": "2023-11-23T03:47:36.814857739Z"
    }
]`, string(result))

}
