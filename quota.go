package dq

import (
	"github.com/TykTechnologies/crdt"
)

type QuotaStatus int

const (
	Quota_violated QuotaStatus = iota
	Quota_ok
	Quota_not_found
)

type Quota struct {
	ID      string
	NodeID  string
	Max     int
	Counter *crdt.GCounter
	Delete  bool
}

func (q *Quota) Used() int {
	return q.Counter.Count()
}

func (q *Quota) Limit() int {
	return q.Max
}

func (q *Quota) IncrBy(c int) QuotaStatus {
	q.Counter.IncVal(c)
	if q.Counter.Count() > q.Max {
		return Quota_violated
	}

	return Quota_ok
}
