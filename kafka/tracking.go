package kafka

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/bsm/redislock"
	redisdb "github.com/go-redis/redis"
	"github.com/pinpt/go-common/eventing"
)

// DefaultIdleDuration is the default duration once we receive EOF for all partitions to determine
// if the consumer group is idle
const DefaultIdleDuration = time.Second

// JobKey is information contained in the job header
type JobKey struct {
	CustomerID string
	JobID      string
	RefType    string
	Topic      string
}

func newJobKey(customerid, jobid, reftype, topic string) JobKey {
	return JobKey{customerid, jobid, reftype, topic}
}

// TrackingConsumerEOF is a handler for receiving the EOF for the consumer group
type EOFCallback interface {
	eventing.ConsumerCallback

	// GroupEOF is called when the consumer group reaches EOF all partitions
	GroupEOF(count int64, jobcounts map[JobKey]int64)
}

// TrackingConsumer is an utility which will track a consumer group and detect when the consumer group
// has it EOF across all the partitions in the consumer group
type TrackingConsumer struct {
	topic          string
	redisPubSubKey string
	redisEOFKey    string
	redisCountKey  string
	redisLockKey   string
	partitions     []int32
	consumer       *Consumer
	redisClient    *redisdb.Client
	pubsub         *redisdb.PubSub
	lockClient     *redislock.Client
	callback       EOFCallback
	assignments    []eventing.TopicPartition
	position       map[int32]int64
	counts         map[int32]int64
	eofs           map[int32]bool
	jobcounts      map[JobKey]int64
	atEOF          bool
	records        int64
	idleduration   time.Duration
	closed         bool
	mu             sync.Mutex
	ctx            context.Context
	cancel         context.CancelFunc
}

// make sure we implement all the interfaces we expect
var _ eventing.ConsumerCallback = (*TrackingConsumer)(nil)
var _ eventing.ConsumerCallbackPartitionLifecycle = (*TrackingConsumer)(nil)
var _ eventing.ConsumerCallbackMessageFilter = (*TrackingConsumer)(nil)
var _ eventing.ConsumerCallbackEventFilter = (*TrackingConsumer)(nil)
var _ ConsumerStatsCallback = (*TrackingConsumer)(nil)
var _ ConsumerEOFCallback = (*TrackingConsumer)(nil)

// Assignments returns the current assignments for this consumer
func (tc *TrackingConsumer) Assignments() []eventing.TopicPartition {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.assignments
}

// Positions returns the per partition consumer offset positions
func (tc *TrackingConsumer) Positions() map[int32]int64 {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.position
}

// AtEOF returns true if the consumer is currently at EOF
func (tc *TrackingConsumer) AtEOF() bool {
	tc.mu.Lock()
	val := tc.atEOF
	tc.mu.Unlock()
	return val
}

// RecordCount returns true current number of records that have been processed
// assuming not EOF
func (tc *TrackingConsumer) RecordCount() int64 {
	tc.mu.Lock()
	val := tc.records
	tc.mu.Unlock()
	return val
}

func (tc *TrackingConsumer) Stats(stats map[string]interface{}) {
	if c, ok := tc.callback.(ConsumerStatsCallback); ok {
		c.Stats(stats)
	}
}

func (tc *TrackingConsumer) Close() error {
	tc.mu.Lock()
	tc.cancel()
	closed := tc.closed
	tc.closed = true
	tc.pubsub.Unsubscribe(tc.redisPubSubKey)
	tc.pubsub.Close()
	tc.pubsub = nil
	defer tc.mu.Unlock()
	if !closed {
		return tc.consumer.Close()
	}
	return nil
}

func (tc *TrackingConsumer) ShouldProcess(o interface{}) bool {
	if c, ok := tc.callback.(eventing.ConsumerCallbackMessageFilter); ok {
		return c.ShouldProcess(o)
	}
	return true
}

func (tc *TrackingConsumer) ShouldFilter(m *eventing.Message) bool {
	if c, ok := tc.callback.(eventing.ConsumerCallbackEventFilter); ok {
		return c.ShouldFilter(m)
	}
	return true
}

func (tc *TrackingConsumer) ErrorReceived(err error) {
	tc.callback.ErrorReceived(err)
}

func (tc *TrackingConsumer) DataReceived(msg eventing.Message) error {
	// fmt.Println("! data", msg.Key, "msg", msg.Headers["message-id"])
	tc.mu.Lock()
	if tc.eofs[msg.Partition] {
		// if we've marked this position as EOF, we need to notify that's no longer EOF
		tc.redisClient.SRem(tc.redisEOFKey, strconv.Itoa(int(msg.Partition)))
	}
	tc.position[msg.Partition] = msg.Offset
	tc.eofs[msg.Partition] = false
	tc.counts[msg.Partition]++
	customerid := msg.Headers["customer_id"]
	jobid := msg.Headers["job_id"]
	reftype := msg.Headers["ref_type"]
	tc.jobcounts[newJobKey(customerid, jobid, reftype, msg.Topic)]++
	tc.atEOF = false
	tc.records++
	tc.mu.Unlock()
	return tc.callback.DataReceived(msg)
}

func (tc *TrackingConsumer) PartitionAssignment(partitions []eventing.TopicPartition) {
	tc.mu.Lock()
	lock, err := tc.obtainGlobalLock()
	if err != nil {
		tc.mu.Unlock()
		tc.ErrorReceived(fmt.Errorf("error obtaining redis global lock for %v. %v", tc.topic, err))
		return
	}
	tc.assignments = partitions
	for _, assignment := range tc.assignments {
		offset := assignment.Offset
		if offset < 0 {
			offset = 0
		}
		tc.eofs[assignment.Partition] = false
		tc.position[assignment.Partition] = offset
	}
	p := tc.redisClient.Pipeline()
	var offset int
	// we need to transfer our partition details from redis in the case another consumer rebalanced
	// and we're picking up their assignments
	for _, partition := range tc.assignments {
		p.HGet(tc.redisCountKey, strconv.Itoa(int(partition.Partition)))
		offset++
	}
	for _, partition := range tc.assignments {
		p.SIsMember(tc.redisEOFKey, partition.Partition)
	}
	res, err := p.Exec()
	if err != nil && err != redisdb.Nil {
		tc.mu.Unlock()
		lock.Release()
		tc.ErrorReceived(fmt.Errorf("error executing assignments for %v. %v", tc.topic, err))
		return
	}
	for i, partition := range tc.assignments {
		count, _ := strconv.ParseInt(res[i].(*redisdb.StringCmd).Val(), 10, 64)
		tc.counts[partition.Partition] = count
		found := res[offset+i].(*redisdb.BoolCmd).Val()
		tc.eofs[partition.Partition] = found
	}
	tc.mu.Unlock()
	lock.Release()

	if c, ok := tc.callback.(eventing.ConsumerCallbackPartitionLifecycle); ok {
		c.PartitionAssignment(partitions)
	}
}

func (tc *TrackingConsumer) PartitionRevocation(partitions []eventing.TopicPartition) {
	tc.mu.Lock()
	lock, err := tc.obtainGlobalLock()
	if err != nil {
		tc.mu.Unlock()
		tc.ErrorReceived(fmt.Errorf("error obtaining redis global lock for %v. %v", tc.topic, err))
		return
	}

	p := tc.redisClient.Pipeline()
	// we need to transfer our partition details back to redis so that the consumer picking them up
	// can start from where we left off
	for _, partition := range tc.assignments {
		total := tc.counts[partition.Partition]
		p.HSet(tc.redisCountKey, strconv.Itoa(int(partition.Partition)), total)
		if tc.eofs[partition.Partition] {
			p.SAdd(tc.redisEOFKey, partition.Partition)
		} else {
			p.SRem(tc.redisEOFKey, partition.Partition)
		}
	}
	if _, err := p.Exec(); err != nil && err != redisdb.Nil {
		tc.mu.Unlock()
		lock.Release()
		tc.ErrorReceived(fmt.Errorf("error executing revocation assignment for %v. %v", tc.topic, err))
		return
	}
	for _, partition := range partitions {
		delete(tc.eofs, partition.Partition)
		delete(tc.position, partition.Partition)
		delete(tc.counts, partition.Partition)
	}
	tc.mu.Unlock()
	lock.Release()

	if c, ok := tc.callback.(eventing.ConsumerCallbackPartitionLifecycle); ok {
		c.PartitionRevocation(partitions)
	}
}

func (tc *TrackingConsumer) OffsetsCommitted(offsets []eventing.TopicPartition) {
	if c, ok := tc.callback.(eventing.ConsumerCallbackPartitionLifecycle); ok {
		c.OffsetsCommitted(offsets)
	}
}

func (tc *TrackingConsumer) obtainGlobalLock() (*redislock.Lock, error) {
	opts := &redislock.Options{
		Context:    context.Background(),
		RetryCount: 5,
	}
	return tc.lockClient.Obtain(tc.redisLockKey, 5*time.Second, opts)
}

func (tc *TrackingConsumer) globalEOF(total int64) {
	tc.mu.Lock()
	p := tc.redisClient.Pipeline()
	// reset our counts
	for partition := range tc.counts {
		tc.counts[partition] = 0
	}
	// reset our partitions in the cloud (only our assignments)
	p = tc.redisClient.Pipeline()
	for _, partition := range tc.assignments {
		p.HDel(tc.redisCountKey, strconv.Itoa(int(partition.Partition)))
	}
	if _, err := p.Exec(); err != nil && err != redisdb.Nil {
		tc.ErrorReceived(fmt.Errorf("error resetting redis values for %v. %v", tc.topic, err))
	}

	tc.records = 0 // reset so that each EOF is delimited by how many records were processed (locally)
	counts := make(map[JobKey]int64)
	for k, v := range tc.jobcounts {
		counts[k] = v
		delete(tc.jobcounts, k)
	}
	tc.mu.Unlock()

	tc.callback.GroupEOF(total, counts)
}

func (tc *TrackingConsumer) EOF(topic string, partition int32, offset int64) {
	// fmt.Println("! EOF", topic, partition, offset)
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.eofs[partition] = true

	// callback after this function returns and w/o holding the lock
	if c, ok := tc.callback.(ConsumerEOFCallback); ok {
		defer c.EOF(topic, partition, offset)
	}

	// detect if all our partitions are EOF
	var eofcount int
	for _, eof := range tc.eofs {
		if eof {
			eofcount++
		}
	}

	// as an optimization, just wait for all our partittions to be empty
	if eofcount == len(tc.eofs) {
		tc.atEOF = true
		if tc.records > 0 {
			if tc.idleduration <= 0 {
				tc.idleduration = DefaultIdleDuration
			}
			// since we can get a message storm with a ton of messages but have small eof events during
			// blocking waiting for the consumer, we're going to use our idle timer to differentiate
			// these cases vs when the partition is truly empty
			go func() {
				// check once more now that we're in a separate goroutine
				// to make sure we're still in an EOF situation
				tc.mu.Lock()
				cancelled := !tc.atEOF
				tc.mu.Unlock()
				if cancelled {
					// since we spun up the goroutine, we've had another record come in
					return
				}
				// wait for our idle duration
				<-time.After(tc.idleduration)
				tc.mu.Lock()
				defer tc.mu.Unlock()
				// check to make sure we're still at EOF when the timer fires, if not,
				// then we still have records coming in
				if tc.atEOF {
					// since we can have multiple partitions, we need to hold the global lock while we do work
					lock, err := tc.obtainGlobalLock()
					if err != nil {
						tc.ErrorReceived(fmt.Errorf("error obtaining redis global lock for %v. %v", tc.topic, err))
						return
					}
					defer lock.Release()

					p := tc.redisClient.Pipeline()
					var scardOffset int
					// set the fact that our partitions are in EOF
					for _, assignment := range tc.assignments {
						p.SAdd(tc.redisEOFKey, assignment.Partition)
						scardOffset++
					}
					// set the total number of records processed for each partition
					for partition, total := range tc.counts {
						p.HSet(tc.redisCountKey, strconv.Itoa(int(partition)), total)
						scardOffset++
					}
					// get the total for all groups in case we're done
					p.SCard(tc.redisEOFKey)
					// get the total number of records in case we're done
					p.HVals(tc.redisCountKey)
					res, err := p.Exec()
					if err != nil {
						tc.ErrorReceived(fmt.Errorf("error checking redis values for %v. %v", tc.topic, err))
						return
					}
					// if our cardinality matches the total number of partitions, all partitions are at EOF
					allEOF := int(res[scardOffset].(*redisdb.IntCmd).Val()) == len(tc.partitions)
					if allEOF {
						// just figure out the total so we can broadcast to other consumers that might be at EOF already
						var totals []int64
						res[1+scardOffset].(*redisdb.StringSliceCmd).ScanSlice(&totals)
						var total int64
						for _, val := range totals {
							total += val
						}
						if total > 0 {
							// we're going to get it from our own publish message
							tc.redisClient.Publish(tc.redisPubSubKey, total)
						}
					} else {
						// we're going to get it from one of our peers via a subscription event
						// nothing to do here then...
					}
				}
			}()
		} else {
			// since we can have multiple physical consumers in the group, we need to hold the global lock while we do work
			lock, err := tc.obtainGlobalLock()
			if err != nil {
				tc.ErrorReceived(fmt.Errorf("error obtaining redis global lock for %v. %v", tc.topic, err))
				return
			}
			defer lock.Release()
			p := tc.redisClient.Pipeline()
			// set the fact that our partitions are in EOF
			for _, assignment := range tc.assignments {
				p.SAdd(tc.redisEOFKey, assignment.Partition)
			}
			if _, err := p.Exec(); err != nil {
				tc.ErrorReceived(fmt.Errorf("error executing redis update on eof for %v. %v", tc.topic, err))
				return
			}
		}
	}
}

func (tc *TrackingConsumer) run() {
	// subscribe to global EOF notification. we need this because we could have N more consumers than ourself
	// and if we do, only one will figure out the global EOF for the entire consumer group (across all partitions)
	// and we will use redis pub sub to communicate across the consumer groups that we've hit the EOF
	tc.pubsub = tc.redisClient.Subscribe(tc.redisPubSubKey)
	ch := tc.pubsub.Channel()
	defer tc.pubsub.Unsubscribe(tc.redisPubSubKey)
	for {
		select {
		case <-tc.ctx.Done():
			return
		case msg := <-ch:
			if msg != nil {
				total, err := strconv.ParseInt(msg.Payload, 10, 64)
				if err != nil {
					tc.ErrorReceived(fmt.Errorf("error parsing redis total on EOF: %v", err))
					return
				}
				tc.globalEOF(total)
			} else {
				return // shutdown
			}
		}
	}
}

// NewTrackingConsumer returns a consumer callback adapter which tracks EOF
func NewTrackingConsumer(topic string, groupID string, config Config, redisClient *redisdb.Client, callback EOFCallback) (*TrackingConsumer, error) {
	c := *(&config)
	c.ClientID = "tracking_" + groupID + "_" + topic
	consumer, err := NewConsumer(c, groupID, topic)
	if err != nil {
		return nil, err
	}
	admin, err := NewAdminClientUsingConsumer(consumer)
	if err != nil {
		return nil, err
	}
	md, err := admin.GetTopic(topic)
	if err != nil {
		return nil, fmt.Errorf("error getting topic '%s' metadata. %v", topic, err)
	}
	partitions := []int32{}
	for _, m := range md.Partitions {
		partitions = append(partitions, m.ID)
	}
	ctx, cancel := context.WithCancel(consumer.config.Context)
	tc := &TrackingConsumer{
		ctx:            ctx,
		cancel:         cancel,
		consumer:       consumer,
		topic:          topic,
		partitions:     partitions,
		redisPubSubKey: fmt.Sprintf("%s_%s$notif", groupID, topic),
		redisEOFKey:    fmt.Sprintf("%s_%s$eof", groupID, topic),
		redisCountKey:  fmt.Sprintf("%s_%s$count", groupID, topic),
		redisLockKey:   fmt.Sprintf("%s_%s$lock", groupID, topic),
		redisClient:    redisClient,
		lockClient:     redislock.New(redisClient),
		callback:       callback,
		position:       make(map[int32]int64),
		eofs:           make(map[int32]bool),
		counts:         make(map[int32]int64),
		jobcounts:      make(map[JobKey]int64),
	}
	go tc.run()          // start the background subscription listener
	consumer.Consume(tc) // start consuming data
	return tc, nil
}

type callbackWithEOF struct {
	*eventing.ConsumerCallbackAdapter
	eof func(total int64, jobcounts map[JobKey]int64)
}

var _ EOFCallback = (*callbackWithEOF)(nil)

func (c *callbackWithEOF) GroupEOF(total int64, jobcounts map[JobKey]int64) {
	c.eof(total, jobcounts)
}

// NewConsumerCallbackWithGroupEOF will create a delegate for handling a ConsumerCallbackAdapter and adding a GroupEOF event as a func handler
func NewConsumerCallbackWithGroupEOF(callback *eventing.ConsumerCallbackAdapter, h func(total int64, jobcounts map[JobKey]int64)) EOFCallback {
	return &callbackWithEOF{callback, h}
}
