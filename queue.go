package migrator

import (
	"fmt"
	"os"
	"sync"

	"github.com/beeker1121/goque"
)

// PersistenceQueue is a wrapper around goque, a LevelDB instance wrapped
// around some usage code. It is used to serialize queue items to disk
// in case of failure.
type PersistenceQueue struct {
	mutex *sync.Mutex
	queue *goque.Queue
}

// OpenQueue creates an instance of a FIFO queue
func OpenQueue(path string) (PersistenceQueue, error) {
	if !FileExists(path) {
		err := os.MkdirAll(path, 0700)
		if err != nil {
			return PersistenceQueue{}, err
		}
	}
	q, err := goque.OpenQueue(path)
	obj := PersistenceQueue{queue: q, mutex: &sync.Mutex{}}
	return obj, err
}

func (pq *PersistenceQueue) AddItem(item interface{}) error {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()
	_, err := pq.queue.EnqueueObject(item)
	return err
}

func (pq *PersistenceQueue) GrabItem(item interface{}, fn func(interface{}) error) error {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()
	x, err := pq.queue.Peek()
	if err != nil {
		return err
	}
	err = x.ToObject(item)
	if err != nil {
		return err
	}
	if fn == nil {
		return fmt.Errorf("nil function passed to PersistenceQueue.Item")
	}
	err = fn(item)
	if err == nil {
		_, err = pq.queue.Dequeue()
	}
	return err
}

func (pq *PersistenceQueue) Close() error {
	return pq.queue.Close()
}
