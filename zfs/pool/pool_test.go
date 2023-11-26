package pool

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestPoolMetrics(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	c := NewCollector(zerolog.Nop())
	reg.MustRegister(c)

	for _, tc := range []struct {
		name string

		expectedMetrics string
	}{
		{
			name: "simple",
			expectedMetrics: `
# HELP zfs_pool_disk_errors_total Total count of ZFS disk errors
# TYPE zfs_pool_disk_errors_total counter
zfs_pool_disk_errors_total{disk="/dev/sda",pool="pool",type="checksum"} 0
zfs_pool_disk_errors_total{disk="/dev/sda",pool="pool",type="read"} 0
zfs_pool_disk_errors_total{disk="/dev/sda",pool="pool",type="write"} 0
# HELP zfs_pool_disk_status Status of a single disk in a ZFS pool
# TYPE zfs_pool_disk_status gauge
zfs_pool_disk_status{disk="/dev/sda",pool="pool",state="degraded"} 0
zfs_pool_disk_status{disk="/dev/sda",pool="pool",state="faulted"} 0
zfs_pool_disk_status{disk="/dev/sda",pool="pool",state="offline"} 0
zfs_pool_disk_status{disk="/dev/sda",pool="pool",state="online"} 1
zfs_pool_disk_status{disk="/dev/sda",pool="pool",state="removed"} 0
zfs_pool_disk_status{disk="/dev/sda",pool="pool",state="unavail"} 0
# HELP zfs_pool_errors_total Total count of ZFS pool errors
# TYPE zfs_pool_errors_total counter
zfs_pool_errors_total{pool="pool",type="checksum"} 0
zfs_pool_errors_total{pool="pool",type="read"} 0
zfs_pool_errors_total{pool="pool",type="write"} 0
# HELP zfs_pool_status Status of ZFS pool
# TYPE zfs_pool_status gauge
zfs_pool_status{pool="pool",state="degraded"} 0
zfs_pool_status{pool="pool",state="faulted"} 0
zfs_pool_status{pool="pool",state="offline"} 0
zfs_pool_status{pool="pool",state="online"} 1
zfs_pool_status{pool="pool",state="removed"} 0
zfs_pool_status{pool="pool",state="unavail"} 0
			`,
		},
		{
			name: "simple-errors",
			expectedMetrics: `
# HELP zfs_pool_disk_errors_total Total count of ZFS disk errors
# TYPE zfs_pool_disk_errors_total counter
zfs_pool_disk_errors_total{disk="/dev/sda",pool="pool",type="checksum"} 3
zfs_pool_disk_errors_total{disk="/dev/sda",pool="pool",type="read"} 1
zfs_pool_disk_errors_total{disk="/dev/sda",pool="pool",type="write"} 2
# HELP zfs_pool_disk_status Status of a single disk in a ZFS pool
# TYPE zfs_pool_disk_status gauge
zfs_pool_disk_status{disk="/dev/sda",pool="pool",state="degraded"} 0
zfs_pool_disk_status{disk="/dev/sda",pool="pool",state="faulted"} 0
zfs_pool_disk_status{disk="/dev/sda",pool="pool",state="offline"} 1
zfs_pool_disk_status{disk="/dev/sda",pool="pool",state="online"} 0
zfs_pool_disk_status{disk="/dev/sda",pool="pool",state="removed"} 0
zfs_pool_disk_status{disk="/dev/sda",pool="pool",state="unavail"} 0
# HELP zfs_pool_errors_total Total count of ZFS pool errors
# TYPE zfs_pool_errors_total counter
zfs_pool_errors_total{pool="pool",type="checksum"} 6
zfs_pool_errors_total{pool="pool",type="read"} 2
zfs_pool_errors_total{pool="pool",type="write"} 4
# HELP zfs_pool_status Status of ZFS pool
# TYPE zfs_pool_status gauge
zfs_pool_status{pool="pool",state="degraded"} 0
zfs_pool_status{pool="pool",state="faulted"} 1
zfs_pool_status{pool="pool",state="offline"} 0
zfs_pool_status{pool="pool",state="online"} 0
zfs_pool_status{pool="pool",state="removed"} 0
zfs_pool_status{pool="pool",state="unavail"} 0
			`,
		},
		{
			name: "multiple-pools",
			expectedMetrics: `
# HELP zfs_pool_status Status of ZFS pool
# TYPE zfs_pool_status gauge
zfs_pool_status{pool="pool-hdd",state="degraded"} 0.0
zfs_pool_status{pool="pool-hdd",state="faulted"} 0.0
zfs_pool_status{pool="pool-hdd",state="offline"} 0.0
zfs_pool_status{pool="pool-hdd",state="online"} 1.0
zfs_pool_status{pool="pool-hdd",state="removed"} 0.0
zfs_pool_status{pool="pool-hdd",state="unavail"} 0.0
zfs_pool_status{pool="pool-nvme",state="degraded"} 0.0
zfs_pool_status{pool="pool-nvme",state="faulted"} 0.0
zfs_pool_status{pool="pool-nvme",state="offline"} 0.0
zfs_pool_status{pool="pool-nvme",state="online"} 1.0
zfs_pool_status{pool="pool-nvme",state="removed"} 0.0
zfs_pool_status{pool="pool-nvme",state="unavail"} 0.0
zfs_pool_status{pool="pool-ssd",state="degraded"} 0.0
zfs_pool_status{pool="pool-ssd",state="faulted"} 0.0
zfs_pool_status{pool="pool-ssd",state="offline"} 0.0
zfs_pool_status{pool="pool-ssd",state="online"} 1.0
zfs_pool_status{pool="pool-ssd",state="removed"} 0.0
zfs_pool_status{pool="pool-ssd",state="unavail"} 0.0
# HELP zfs_pool_errors_total Total count of ZFS pool errors
# TYPE zfs_pool_errors_total counter
zfs_pool_errors_total{pool="pool-hdd",type="read"} 0.0
zfs_pool_errors_total{pool="pool-hdd",type="write"} 0.0
zfs_pool_errors_total{pool="pool-hdd",type="checksum"} 0.0
zfs_pool_errors_total{pool="pool-nvme",type="read"} 0.0
zfs_pool_errors_total{pool="pool-nvme",type="write"} 0.0
zfs_pool_errors_total{pool="pool-nvme",type="checksum"} 0.0
zfs_pool_errors_total{pool="pool-ssd",type="read"} 0.0
zfs_pool_errors_total{pool="pool-ssd",type="write"} 0.0
zfs_pool_errors_total{pool="pool-ssd",type="checksum"} 0.0
# HELP zfs_pool_disk_status Status of a single disk in a ZFS pool
# TYPE zfs_pool_disk_status gauge
zfs_pool_disk_status{disk="/dev/disk/by-id/dm-uuid-CRYPT-LUKS2-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",pool="pool-hdd",state="degraded"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/dm-uuid-CRYPT-LUKS2-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",pool="pool-hdd",state="faulted"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/dm-uuid-CRYPT-LUKS2-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",pool="pool-hdd",state="offline"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/dm-uuid-CRYPT-LUKS2-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",pool="pool-hdd",state="online"} 1.0
zfs_pool_disk_status{disk="/dev/disk/by-id/dm-uuid-CRYPT-LUKS2-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",pool="pool-hdd",state="removed"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/dm-uuid-CRYPT-LUKS2-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",pool="pool-hdd",state="unavail"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/dm-name-yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy",pool="pool-nvme",state="degraded"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/dm-name-yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy",pool="pool-nvme",state="faulted"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/dm-name-yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy",pool="pool-nvme",state="offline"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/dm-name-yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy",pool="pool-nvme",state="online"} 1.0
zfs_pool_disk_status{disk="/dev/disk/by-id/dm-name-yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy",pool="pool-nvme",state="removed"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/dm-name-yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy",pool="pool-nvme",state="unavail"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/dm-name-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",pool="pool-ssd",state="degraded"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/dm-name-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",pool="pool-ssd",state="faulted"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/dm-name-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",pool="pool-ssd",state="offline"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/dm-name-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",pool="pool-ssd",state="online"} 1.0
zfs_pool_disk_status{disk="/dev/disk/by-id/dm-name-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",pool="pool-ssd",state="removed"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/dm-name-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",pool="pool-ssd",state="unavail"} 0.0
# HELP zfs_pool_disk_errors_total Total count of ZFS disk errors
# TYPE zfs_pool_disk_errors_total counter
zfs_pool_disk_errors_total{disk="/dev/disk/by-id/dm-uuid-CRYPT-LUKS2-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",pool="pool-hdd",type="read"} 0.0
zfs_pool_disk_errors_total{disk="/dev/disk/by-id/dm-uuid-CRYPT-LUKS2-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",pool="pool-hdd",type="write"} 0.0
zfs_pool_disk_errors_total{disk="/dev/disk/by-id/dm-uuid-CRYPT-LUKS2-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",pool="pool-hdd",type="checksum"} 0.0
zfs_pool_disk_errors_total{disk="/dev/disk/by-id/dm-name-yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy",pool="pool-nvme",type="read"} 0.0
zfs_pool_disk_errors_total{disk="/dev/disk/by-id/dm-name-yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy",pool="pool-nvme",type="write"} 0.0
zfs_pool_disk_errors_total{disk="/dev/disk/by-id/dm-name-yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy",pool="pool-nvme",type="checksum"} 0.0
zfs_pool_disk_errors_total{disk="/dev/disk/by-id/dm-name-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",pool="pool-ssd",type="read"} 0.0
zfs_pool_disk_errors_total{disk="/dev/disk/by-id/dm-name-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",pool="pool-ssd",type="write"} 0.0
zfs_pool_disk_errors_total{disk="/dev/disk/by-id/dm-name-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",pool="pool-ssd",type="checksum"} 0.0
`,
		},
		{
			name: "raidz",
			expectedMetrics: `
# HELP zfs_pool_status Status of ZFS pool
# TYPE zfs_pool_status gauge
zfs_pool_status{pool="rpool",state="degraded"} 0.0
zfs_pool_status{pool="rpool",state="faulted"} 0.0
zfs_pool_status{pool="rpool",state="offline"} 0.0
zfs_pool_status{pool="rpool",state="online"} 1.0
zfs_pool_status{pool="rpool",state="removed"} 0.0
zfs_pool_status{pool="rpool",state="unavail"} 0.0
zfs_pool_status{pool="rpool/raidz1-0",state="degraded"} 0.0
zfs_pool_status{pool="rpool/raidz1-0",state="faulted"} 0.0
zfs_pool_status{pool="rpool/raidz1-0",state="offline"} 0.0
zfs_pool_status{pool="rpool/raidz1-0",state="online"} 1.0
zfs_pool_status{pool="rpool/raidz1-0",state="removed"} 0.0
zfs_pool_status{pool="rpool/raidz1-0",state="unavail"} 0.0
# HELP zfs_pool_disk_status Status of a single disk in a ZFS pool
# TYPE zfs_pool_disk_status gauge
zfs_pool_disk_status{disk="/dev/disk/by-id/id1-part4",pool="rpool/raidz1-0",state="degraded"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/id1-part4",pool="rpool/raidz1-0",state="faulted"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/id1-part4",pool="rpool/raidz1-0",state="offline"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/id1-part4",pool="rpool/raidz1-0",state="online"} 1.0
zfs_pool_disk_status{disk="/dev/disk/by-id/id1-part4",pool="rpool/raidz1-0",state="removed"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/id1-part4",pool="rpool/raidz1-0",state="unavail"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/id2-part4",pool="rpool/raidz1-0",state="degraded"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/id2-part4",pool="rpool/raidz1-0",state="faulted"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/id2-part4",pool="rpool/raidz1-0",state="offline"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/id2-part4",pool="rpool/raidz1-0",state="online"} 1.0
zfs_pool_disk_status{disk="/dev/disk/by-id/id2-part4",pool="rpool/raidz1-0",state="removed"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/id2-part4",pool="rpool/raidz1-0",state="unavail"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/id3-part4",pool="rpool/raidz1-0",state="degraded"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/id3-part4",pool="rpool/raidz1-0",state="faulted"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/id3-part4",pool="rpool/raidz1-0",state="offline"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/id3-part4",pool="rpool/raidz1-0",state="online"} 1.0
zfs_pool_disk_status{disk="/dev/disk/by-id/id3-part4",pool="rpool/raidz1-0",state="removed"} 0.0
zfs_pool_disk_status{disk="/dev/disk/by-id/id3-part4",pool="rpool/raidz1-0",state="unavail"} 0.0
zfs_pool_disk_status{disk="/dev/sda3",pool="rpool/cache",state="degraded"} 0.0
zfs_pool_disk_status{disk="/dev/sda3",pool="rpool/cache",state="faulted"} 0.0
zfs_pool_disk_status{disk="/dev/sda3",pool="rpool/cache",state="offline"} 0.0
zfs_pool_disk_status{disk="/dev/sda3",pool="rpool/cache",state="online"} 1.0
zfs_pool_disk_status{disk="/dev/sda3",pool="rpool/cache",state="removed"} 0.0
zfs_pool_disk_status{disk="/dev/sda3",pool="rpool/cache",state="unavail"} 0.0
# HELP zfs_pool_errors_total Total count of ZFS pool errors
# TYPE zfs_pool_errors_total counter
zfs_pool_errors_total{pool="rpool",type="read"} 0.0
zfs_pool_errors_total{pool="rpool",type="write"} 0.0
zfs_pool_errors_total{pool="rpool",type="checksum"} 0.0
zfs_pool_errors_total{pool="rpool/raidz1-0",type="read"} 0.0
zfs_pool_errors_total{pool="rpool/raidz1-0",type="write"} 0.0
zfs_pool_errors_total{pool="rpool/raidz1-0",type="checksum"} 0.0
# HELP zfs_pool_disk_errors_total Total count of ZFS disk errors
# TYPE zfs_pool_disk_errors_total counter
zfs_pool_disk_errors_total{disk="/dev/disk/by-id/id1-part4",pool="rpool/raidz1-0",type="read"} 0.0
zfs_pool_disk_errors_total{disk="/dev/disk/by-id/id1-part4",pool="rpool/raidz1-0",type="write"} 0.0
zfs_pool_disk_errors_total{disk="/dev/disk/by-id/id1-part4",pool="rpool/raidz1-0",type="checksum"} 0.0
zfs_pool_disk_errors_total{disk="/dev/disk/by-id/id2-part4",pool="rpool/raidz1-0",type="read"} 0.0
zfs_pool_disk_errors_total{disk="/dev/disk/by-id/id2-part4",pool="rpool/raidz1-0",type="write"} 0.0
zfs_pool_disk_errors_total{disk="/dev/disk/by-id/id2-part4",pool="rpool/raidz1-0",type="checksum"} 0.0
zfs_pool_disk_errors_total{disk="/dev/disk/by-id/id3-part4",pool="rpool/raidz1-0",type="read"} 0.0
zfs_pool_disk_errors_total{disk="/dev/disk/by-id/id3-part4",pool="rpool/raidz1-0",type="write"} 0.0
zfs_pool_disk_errors_total{disk="/dev/disk/by-id/id3-part4",pool="rpool/raidz1-0",type="checksum"} 0.0
zfs_pool_disk_errors_total{disk="/dev/sda3",pool="rpool/cache",type="read"} 0.0
zfs_pool_disk_errors_total{disk="/dev/sda3",pool="rpool/cache",type="write"} 0.0
zfs_pool_disk_errors_total{disk="/dev/sda3",pool="rpool/cache",type="checksum"} 0.0
			`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("testdata", tc.name+".txt"))
			require.NoError(t, err)
			c.getStatus = func() ([]byte, error) {
				return data, nil
			}

			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(tc.expectedMetrics)))
			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(tc.expectedMetrics)))
		})
	}
}
