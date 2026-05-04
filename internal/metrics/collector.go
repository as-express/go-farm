package metrics

import (
	"sync"
	"time"
)

var Metrics = NewCollector()

type Collector struct {
	mu sync.Mutex

	TasksAll     int64
	TasksSuccess int64
	TasksFail    int64

	HttpRequests int64
	HttpErrors   int64
	HttpLatency  int64

	ErrorMap map[string]int64
}

func NewCollector() *Collector {
	return &Collector{
		ErrorMap: make(map[string]int64),
	}
}

func (c *Collector) RecordTask() {
	c.mu.Lock()
	c.TasksAll++
	c.mu.Unlock()
}

func (c *Collector) RecordSuccess() {
	c.mu.Lock()
	c.TasksSuccess++
	c.mu.Unlock()
}

func (c *Collector) RecordFail() {
	c.mu.Lock()
	c.TasksFail++
	c.mu.Unlock()
}

func (c *Collector) RecordHTTP(ms int64, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.HttpRequests++
	c.HttpLatency += ms

	if err != nil {
		c.HttpErrors++
		c.ErrorMap[err.Error()]++
	}
}

func (c *Collector) Snapshot() map[string]interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	lat := int64(0)

	if c.HttpRequests > 0 {
		lat = c.HttpLatency / c.HttpRequests
	}

	return map[string]interface{}{
		"tasks": map[string]interface{}{
			"all":     c.TasksAll,
			"success": c.TasksSuccess,
			"fail":    c.TasksFail,
		},
		"network": map[string]interface{}{
			"requests":  c.HttpRequests,
			"errors":    c.HttpErrors,
			"latency":   lat,
			"errorsMap": c.ErrorMap,
		},
		"systemServer": map[string]interface{}{
			"uptimeSec": time.Now().Unix(),
		},
	}
}

func (c *Collector) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.TasksAll = 0
	c.TasksSuccess = 0
	c.TasksFail = 0

	c.HttpRequests = 0
	c.HttpErrors = 0
	c.HttpLatency = 0

	c.ErrorMap = make(map[string]int64)
}