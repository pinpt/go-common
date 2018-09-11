Cluster package allows load balancing of read-only queries across multiple database instances. Designed to work with AWS RDS read slave autoscaling.

Details

- queries information_schema.replica_host_status table discovering new or removing no longer available replicas

- since replica could be removed at any time or restarted, retries failed connections against a diffferent instance

- does not retry queries that failed while in progress, due to hard server shutdown and similar