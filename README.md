# zfs-event-exporter

This is an Prometheus exporter to see the status of ZFS pools and ZFS dataset snapshots.

## Summary

This is an improved version of a [polling script], which got too resource intensive to run on big ZFS set-ups. This exporter instead watches ZFS events and records the changes in the exporter state.

[polling script]:https://github.com/simonswine/node-exporter-textfile-collector-scripts/blob/fb831ed78c7c4321b1d897ddc906e274f79e4e30/zfs.py
