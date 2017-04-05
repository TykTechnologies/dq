package dq

import (
	"errors"
	"github.com/TykTechnologies/crdt"
	"github.com/TykTechnologies/tyk-cluster-framework/client"
	"github.com/TykTechnologies/tyk-cluster-framework/payloads"
	"time"
)

type connectionConf struct {
	client   client.Client
	topic    string
	interval time.Duration
}

type DistributedQuota struct {
	nodeID             string
	knownNodes         *Cache
	IsLeader           bool
	conn               connectionConf
	errChan            chan error
	quotas             map[string]*Quota
	flushWith          Flusher
	FlushInterval      time.Duration
	handleChannelError ErrorHandler
}

type Flusher func(map[string]*Quota) error
type ErrorHandler func(err error)

func NewDQ(f Flusher, e ErrorHandler, nid string) *DistributedQuota {
	d := &DistributedQuota{
		nodeID:             nid,
		knownNodes:         NewCache(60 * time.Second),
		errChan:            make(chan error),
		quotas:             make(map[string]*Quota),
		flushWith:          f,
		handleChannelError: e,
		FlushInterval:      time.Second * 60,
	}

	d.InitQuota(0, 0, "default-dq-quota")

	return d
}

func (d *DistributedQuota) SetLeader(t bool) {
	d.IsLeader = t
}

func (d *DistributedQuota) broadcast(errChan chan error) {
	if d.conn.client == nil {
		errChan <- errors.New("Config not set")
	}

	var ticker <-chan time.Time
	ticker = time.After(d.conn.interval)

	for {
		select {
		case <-ticker:
			p, plErr := payloads.NewPayload(d.quotas)
			if plErr != nil {
				errChan <- plErr
			}

			if pErr := d.conn.client.Publish(d.conn.topic, p); pErr != nil {
				errChan <- pErr
			}

			ticker = time.After(d.conn.interval)
		}
	}

}

func (d *DistributedQuota) BroadcastWith(c client.Client, interval time.Duration, topic string) {
	d.conn = connectionConf{
		client:   c,
		interval: interval,
		topic:    topic,
	}
}

func (d *DistributedQuota) listenForCounters(errorChan chan error) error {
	if _, err := d.conn.client.Subscribe(d.conn.topic, func(payload payloads.Payload) {
		var remoteCounters map[string]*Quota
		err := payload.DecodeMessage(&remoteCounters)
		if err != nil {
			errorChan <- err
		}

		// Merge all remote counters with the ones we already have
		var skip bool
		for k, v := range remoteCounters {

			if v.Delete {
				d.Delete(v.ID)
				skip = true
			}

			// If it's deleted, do not process it
			if !skip {
				// Ignore our own
				if v.NodeID != d.nodeID {
					count, f := d.quotas[k]
					if f {
						count.Counter.Merge(v.Counter)
					}
				}

				// Ensure we actually have this node tracked
				d.knownNodes.Set(v.NodeID, v.NodeID)
			}
		}

	}); err != nil {
		return err
	}

	return nil
}

func (d *DistributedQuota) Start() error {
	d.errChan = make(chan error)

	if err := d.conn.client.Connect(); err != nil {
		return err
	}

	go d.handleChannelErrors(d.errChan)
	go d.persistCounters(d.errChan)

	if err := d.listenForCounters(d.errChan); err != nil {
		return err
	}

	time.Sleep(time.Millisecond * 500)
	go d.broadcast(d.errChan)

	return nil

}

func (d *DistributedQuota) handleChannelErrors(errChan chan error) {
	for {
		select {
		case e := <-errChan:
			d.handleChannelError(e)
		}
	}
}

func (d *DistributedQuota) Delete(id string) {
	delete(d.quotas, id)
}

func (d *DistributedQuota) InitQuota(max, used_quota int, id string) {
	q, f := d.quotas[id]
	if !f {
		q = &Quota{ID: id, Max: 0, Counter: crdt.NewGCounter(), NodeID: d.nodeID}
		d.quotas[id] = q
	}

	if q.Max == 0 {
		q.Max = max
	}

	if q.Counter.Count() == 0 {
		divBy := d.knownNodes.Count()
		if d.knownNodes.Count() == 0 {
			divBy = 1
		}
		distributedInit := used_quota / divBy

		q.Counter.IncVal(distributedInit)
	}
}

func (d *DistributedQuota) IncrBy(id string, c int) QuotaStatus {
	_, f := d.quotas[id]
	if f {
		return d.quotas[id].IncrBy(c)
	}
	return quota_not_found
}

func (d *DistributedQuota) persistCounters(errChan chan error) {
	for {
		time.Sleep(d.FlushInterval)
		if d.IsLeader {
			if len(d.quotas) > 0 {
				err := d.flushWith(d.quotas)
				if err != nil {
					errChan <- err
				}
			}
		}
	}
}