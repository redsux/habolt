package habolt

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
)

const (
	retainSnapshotCount = 2
	raftStoreFileName   = "raft.db"
	raftTimeout         = 10 * time.Second
)

type HaStore struct {
	mutex	   sync.Mutex
	
	//Store	   *Store

	Address    string
	Port       int

	LogOutput  io.Writer

	raftServer *raft.Raft
	serfServer *serf.Serf

	serfEvents chan serf.Event
}

func NewHaStore(addr string, port int) (*HaStore, error) {
	obj := &HaStore{
		Address: addr,
		Port: port,
		LogOutput: NewHaOuput(LVL_INFO),
	}
	if err := obj.initSerf(); err != nil {
		return nil, err
	}
	if err := obj.initRaft(); err != nil {
		return nil, err
	}
	return obj, nil
}

func (has *HaStore) SerfAddr() string {
	return fmt.Sprintf("%s:%d", has.Address, has.Port)
}

func (has *HaStore) RaftAddr() string {
	return fmt.Sprintf("%s:%d", has.Address, has.Port + 1)
}

func (has *HaStore) Start(peers ...string) error {
	if len(peers) > 0 {
		if _, err := has.serfServer.Join(peers, false); err != nil {
			return err
		}
	}
	if err := has.raftBootstrap(peers...); err != nil {
		return err
	}

	for {
		select {
		case ev := <-has.serfEvents:
			leader := has.raftServer.VerifyLeader()
			if leader.Error() == nil {
				switch evt := ev.(type) {
				case serf.MemberEvent :		
					if err := has.serfMemberListener(evt); err != nil {
						return err
					}
				case serf.UserEvent :
					if fut := has.raftServer.Apply(evt.Payload, raftTimeout); fut.Error() != nil {
						// ?
					}
				}
			}
		}
	}
}

func (has *HaStore) List() error {
	return nil
}

func (has *HaStore) Get(key string) error {
	return nil
}

func (has *HaStore) Set(key, value string) error {
	msg := fmt.Sprintf(`{ "from": "%v", "key": "%s", "value": "%s" }`, has.Port, key, value)
	return has.serfServer.UserEvent("set", []byte(msg), false)
}

func (has *HaStore) Delete(key string) error {
	return nil
}