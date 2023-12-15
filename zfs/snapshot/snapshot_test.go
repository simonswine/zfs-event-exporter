package snapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func retryMax(t *testing.T, max int, f func() error) error {
	var err error
	for i := 0; i < max; i++ {
		err = f()
		if err == nil {
			break
		}
		if max == 0 {
			return err
		}
		max--
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

func TestPoolMetrics(t *testing.T) {
	var (
		callback func(ctx context.Context, args ...string) ([]byte, error)
		reg      = prometheus.NewPedanticRegistry()
		eventCh  = make(chan *zpoolEvent)
	)

	t.Run("static snapshots after start up", func(t *testing.T) {
		data, err := os.ReadFile(filepath.Join("testdata", "snapshots-simple.txt"))
		require.NoError(t, err)
		callback = func(context.Context, ...string) ([]byte, error) {
			return data, nil
		}

		ctx := context.Background()
		c, err := newCollector(ctx, zerolog.Nop(), func(ctx context.Context, args ...string) ([]byte, error) { return callback(ctx, args...) }, eventCh, func(_, _ string) bool { return true })
		require.NoError(t, err)
		reg.MustRegister(c)

		expectedMetrics := `
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
			`
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics)))
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics)))
	})

	t.Run("add additional snapshot", func(t *testing.T) {
		callback = func(_ context.Context, args ...string) ([]byte, error) {
			require.Contains(t, args, "pool-nvme/data")
			return []byte("pool-nvme/data@migrate_v3	1700000000	4000000\n"), nil
		}
		// prepare data call
		eventCh <- &zpoolEvent{
			HistoryInternalName: "snapshot",
			HistoryDSName:       "pool-nvme/data@migrate_v3",
			Time:                time.Now(), // not really used
		}

		expectedMetrics := `
# HELP zfs_snapshot_count Count of existing ZFS snapshots.
# TYPE zfs_snapshot_count gauge
zfs_snapshot_count{dataset="pool-hdd/backup/pull/node-a/data"} 2
zfs_snapshot_count{dataset="pool-nvme/data"} 3
# HELP zfs_snapshot_disk_used Disk space used by all snapshots.
# TYPE zfs_snapshot_disk_used gauge
zfs_snapshot_disk_used{dataset="pool-hdd/backup/pull/node-a/data"} 24772608
zfs_snapshot_disk_used{dataset="pool-nvme/data"} 7571712
# HELP zfs_snapshot_last_unixtime Time of last ZFS snapshot
# TYPE zfs_snapshot_last_unixtime gauge
zfs_snapshot_last_unixtime{dataset="pool-hdd/backup/pull/node-a/data"} 1667320886
zfs_snapshot_last_unixtime{dataset="pool-nvme/data"} 1700000000
			`
		require.NoError(t, retryMax(t, 10, func() error {
			return testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics))
		}))
	})

	t.Run("delete snapshot", func(t *testing.T) {
		callback = func(_ context.Context, args ...string) ([]byte, error) {
			panic("should not be called")
		}
		// prepare data call
		eventCh <- &zpoolEvent{
			HistoryInternalName: "destroy",
			HistoryDSName:       "pool-nvme/data@migrate_v1",
			Time:                time.Now(), // not really used
		}

		expectedMetrics := `
# HELP zfs_snapshot_count Count of existing ZFS snapshots.
# TYPE zfs_snapshot_count gauge
zfs_snapshot_count{dataset="pool-hdd/backup/pull/node-a/data"} 2
zfs_snapshot_count{dataset="pool-nvme/data"} 3
# HELP zfs_snapshot_disk_used Disk space used by all snapshots.
# TYPE zfs_snapshot_disk_used gauge
zfs_snapshot_disk_used{dataset="pool-hdd/backup/pull/node-a/data"} 24772608
zfs_snapshot_disk_used{dataset="pool-nvme/data"} 7571712
# HELP zfs_snapshot_last_unixtime Time of last ZFS snapshot
# TYPE zfs_snapshot_last_unixtime gauge
zfs_snapshot_last_unixtime{dataset="pool-hdd/backup/pull/node-a/data"} 1667320886
zfs_snapshot_last_unixtime{dataset="pool-nvme/data"} 1700000000
			`

		require.NoError(t, retryMax(t, 10, func() error {
			return testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics))
		}))

	})
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
