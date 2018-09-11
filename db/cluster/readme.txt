Allow load balancing of read-only queries across multiple database instances. Designed to work with AWS RDS read slave autoscaling.

Has the following features:

- queries information_schema.replica_host_status table discovering new or removing no longer available replicas

- TODO: since replica could be terminated at any time when the cluster is scaling down, automatically retries failed query against a different instance

- TODO: query retries in general increase availability, without need of changing application layer

- TODO: marks unresponsive replicas as down for a specific time period
