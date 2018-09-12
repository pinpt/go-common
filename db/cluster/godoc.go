// Package cluster provides load balancing of read-only queries across multiple database instances. Designed to work with AWS RDS Aurora read slaves auto-scaling.
//
// Details
//
// - queries information_schema.replica_host_status table discovering new or removing no longer available replicas
//
// - supports replica restarts and deletes, retries connections failed this way against a different instance
//
// - does not retry queries that failed in progress (due to hard server shutdowns, network interruptions)
package cluster
