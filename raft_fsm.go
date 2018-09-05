package habolt

import (
	"encoding/json"
	"io"
	"github.com/hashicorp/raft"
)

type command struct {
	Op    string	  `json:"op"`
	Key   string      `json:"key"`
	Value interface{} `json:"value,omitempty"`
	Addr  string      `json:"addr,omitempty"`
}

type fsm HaStore

func (f *fsm) Apply(l *raft.Log) interface{} {
	f.store.logger.Printf( "[DEBUG] raft-fsm: Apply %s\n", string(l.Data) )
	var (
		c command
		e error
	)
	if err := json.Unmarshal(l.Data, &c); err != nil {
		f.store.logger.Printf( "[ERR] raft-fsm: Failed to unmarshal command: %s", err.Error())
	}
	f.mutex.Lock()
	defer f.mutex.Unlock()

	switch c.Op {
	case "set":
		if c.Value != nil {
			e = f.store.Set(c.Key, c.Value)
		}
	case "delete":
		e = f.store.Delete(c.Key)
	default:
		f.store.logger.Printf( "[ERR] raft-fsm: Unrecognized command op: %s", c.Op)
	}

	return e
}

// Snapshot returns a snapshot of the key-value store. We wrap
// the things we need in fsmSnapshot and then send that over to Persist.
// Persist encodes the needed data from fsmsnapshot and transport it to
// Restore where the necessary data is replicated into the finite state machine.
// This allows the consensus algorithm to truncate the replicated log.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	tx, err := f.store.conn.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Clone the kvstore into a map for easy transport
	mapClone := make(map[string]string)
	
	curs := tx.Bucket(f.store.bucket).Cursor()
	for key, val := curs.First(); key != nil; key, val = curs.Next() {
		mapClone[ string(key) ] = string(val)
	}

	return &fsmSnapshot{kvMap: mapClone}, tx.Commit()
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(kvMap io.ReadCloser) error {
	kvSnapshot := make(map[string]string)
	if err := json.NewDecoder(kvMap).Decode(&kvSnapshot); err != nil {
		return err
	}
	// f.mutex.Lock()
	// defer f.mutex.Unlock()
	
	tx, err := f.store.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	
	if err := tx.DeleteBucket(f.store.bucket); err != nil {
		return err
	}

	bucket, err := tx.CreateBucketIfNotExists(f.store.bucket)
	if err != nil {
		return err
	}

	for k, v := range kvSnapshot {
		if err := bucket.Put( []byte(k), []byte(v) ); err != nil {
			return err
		}
	}

	return tx.Commit()
}

type fsmSnapshot struct {
	kvMap map[string]string
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f.kvMap)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		if err := sink.Close(); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (f *fsmSnapshot) Release() {}