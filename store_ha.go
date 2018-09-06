package habolt

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
)

const (
	retainSnapshotCount = 2
	raftStoreFileName   = "raft.db"
	raftTimeout         = 10 * time.Second
	raftUserEventName   = "us_hastore_apply"
)

// HaStore is a wrapper of our StaticStore with Serf & Raft
// running to replicate all data between nodes.
type HaStore struct {
	mutex      sync.Mutex
	store      Store
	Bind       *HaAddress
	Advertise  *HaAddress
	raftServer *raft.Raft
	serfServer *serf.Serf
	serfEvents chan serf.Event
}

// NewHaStore create a new HaStore, "bindAddr" will be the local IP:PORT listening address
// "advAddr" will be the advertised IP:PORT address (for NAT Traversal)
func NewHaStore(bindAddr, advAddr *HaAddress, opts *Options) (*HaStore, error) {
	db, err := NewStaticStore(opts)
	if err != nil {
		return nil, err
	}
	obj := &HaStore{
		store:     db,
		Bind:      bindAddr,
		Advertise: advAddr,
	}

	obj.store.Logger().Printf(`[INFO] Starting HaStore servers:
	- Serf listening on %s (%s)
	- Raft listening on %s (%s)`,
		bindAddr, obj.realAddr(),
		bindAddr.Raft(), obj.realAddr().Raft(),
	)

	if err := obj.initSerf(); err != nil {
		return nil, err
	}
	if err := obj.initRaft(); err != nil {
		return nil, err
	}
	return obj, nil
}

// Close to close the embeded Store
func (has *HaStore) Close() error {
	return has.store.Close()
}

// Logger return the logger of our Store (to implements Store interface)
func (has *HaStore) Logger() *log.Logger {
	return has.store.Logger()
}

// LogLevel to change the log level value (LVL_INFO by DEFAULT)
func (has *HaStore) LogLevel(level int) {
	has.store.LogLevel(level)
}

func (has *HaStore) realAddr() *HaAddress {
	if has.Advertise == nil || has.Advertise.Address == "" {
		return has.Bind
	}
	return has.Advertise
}

// Start run the Serf & Raft communication and event handlers
// You could pass some node's addresses "ip:port" in parameters to join an existing cluster
func (has *HaStore) Start(peers ...string) error {
	if len(peers) > 0 {
		if _, err := has.serfServer.Join(peers, false); err != nil {
			return err
		}
	} else {
		if err := has.raftBootstrap(peers...); err != nil {
			return err
		}
	}

	for {
		select {
		case ev := <-has.serfEvents:
			leader := has.raftServer.VerifyLeader()
			if leader.Error() == nil {
				switch evt := ev.(type) {
				case serf.MemberEvent:
					if err := has.serfMemberListener(evt); err != nil {
						return err
					}
				case serf.UserEvent:
					if evt.Name == raftUserEventName {
						fut := has.raftServer.Apply(evt.Payload, raftTimeout)
						if err := fut.Error(); err != nil {
							has.store.Logger().Printf("[DEBUG] raft: Error apply > %v", err)
						}
					}
				}
			}
		}
	}
}

// Members retrieved the list of addresses in our Raft cluster
func (has *HaStore) Members() ([]HaAddress, error) {
	cFuture := has.raftServer.GetConfiguration()
	if err := cFuture.Error(); err != nil {
		return nil, err
	}
	members := make([]HaAddress, 0)
	config := cFuture.Configuration()
	for _, server := range config.Servers {
		addr, err := NewListen(string(server.ID))
		if err != nil {
			return nil, err
		}
		members = append(members, *addr)
	}
	return members, nil
}

// ListRaw retreive all key-values as a string without any modification
func (has *HaStore) ListRaw() (map[string]string, error) {
	return has.store.ListRaw()
}

// List retreive all values in our Store, you could filter by keys with wildcard patterns (i.e. "prefix_*")
func (has *HaStore) List(values interface{}, patterns ...string) error {
	has.mutex.Lock()
	defer has.mutex.Unlock()
	return has.store.List(values, patterns...)
}

// Get retreive a specific value in our Store thanks its key.
func (has *HaStore) Get(key string, value interface{}) error {
	has.mutex.Lock()
	defer has.mutex.Unlock()
	return has.store.Get(key, value)
}

// Set forward the "key"/"value" as an UserEvent to our Serf cluster
// This event will be catched by the Leader of the Raft server to apply it everywhere
func (has *HaStore) Set(key string, value interface{}) error {
	c := &command{
		Op:    "set",
		Key:   key,
		Value: value,
		Addr:  has.realAddr().Raft().String(),
	}
	msg, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return has.serfServer.UserEvent(raftUserEventName, msg, false)
}

// Delete forware the "key"/"value" as an UserEvent to our Serf cluster
// This event will be catched by the Leader of the Raft server to apply it everywhere
func (has *HaStore) Delete(key string) error {
	c := &command{
		Op:   "del",
		Key:  key,
		Addr: has.realAddr().Raft().String(),
	}
	msg, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return has.serfServer.UserEvent(raftUserEventName, msg, false)
}
