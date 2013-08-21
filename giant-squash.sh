#!/bin/sh
java -Xmx3G -jar ./giant-squash-1.0-jar-with-dependencies.jar -hbaseRoot /hbase -interval 60 -output ./giant-squashes.json -tableNames kiji.default.table.site_scale kiji.default.table.site_stream_scale kiji.default.table.bill_interval_read_log_scale kiji.default.table.fixed_interval_read_log_scale
