package habolt

import (
	"encoding/json"
	"io"

	"github.com/hashicorp/raft"
)

type command struct {
	Op    string      `json:"op"`
	Key   string      `json:"key"`
	Value interface{} `json:"value,omitempty"`
	Addr  string      `json:"addr,omitempty"`
}

type fsm struct {
	*HaStore
}

func (f *fsm) Apply(l *raft.Log) interface{} {
	f.Logger().Printf("[DEBUG] fsm: Apply %s\n", string(l.Data))
	var (
		c command
		e error
	)
	if err := json.Unmarshal(l.Data, &c); err != nil {
		f.Logger().Printf("[ERR] fsm: Failed to unmarshal command: %s", err.Error())
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
		f.Logger().Printf("[ERR] fsm: Unrecognized command op: %s", c.Op)
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

	content, err := f.ListRaw()
	if err != nil {
		return nil, err
	}

	return &fsmSnapshot{kvMap: content}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(kvMap io.ReadCloser) error {
	kvSnapshot := make(map[string]string)
	if err := json.NewDecoder(kvMap).Decode(&kvSnapshot); err != nil {
		return err
	}

	for k, v := range kvSnapshot {
		if err := f.store.Set(k, v); err != nil {
			return err
		}
	}

	return nil
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
