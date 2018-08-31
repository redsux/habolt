package habolt

import (
	//"encoding/json"
	"fmt"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
)

type command struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
	Addr  string `json:"addr,omitempty"`
}

func (has *HaStore) initSerf() (err error) {
	has.serfEvents = make(chan serf.Event, 16)

	memberlistConfig := memberlist.DefaultLANConfig()
	memberlistConfig.BindAddr = has.Address
	memberlistConfig.BindPort = has.Port
	memberlistConfig.LogOutput = has.LogOutput

	serfConfig := serf.DefaultConfig()
	serfConfig.NodeName = has.SerfAddr()
	serfConfig.EventCh = has.serfEvents
	serfConfig.MemberlistConfig = memberlistConfig
	serfConfig.LogOutput = has.LogOutput

	has.serfServer, err = serf.Create(serfConfig)
	return
}

func (has *HaStore) serfMemberListener(evt serf.MemberEvent) error {
	for _, member := range evt.Members {
		var action raft.Future

		changedPeer := fmt.Sprintf("%s:%d", member.Addr.String(), member.Port + 1)
		peerId := raft.ServerID(changedPeer)
		peerAddr := raft.ServerAddress(changedPeer)

		switch evt.EventType() {
		case serf.EventMemberJoin:
			action = has.raftServer.AddVoter(peerId, peerAddr, 0, 0)
		case serf.EventMemberFailed:
			fallthrough
		case serf.EventMemberReap:
			fallthrough
		case serf.EventMemberLeave:
			action = has.raftServer.RemoveServer(peerId, 0, 0)
		}

		if action != nil {
			if err := action.Error(); err != nil {
				return err
			}
		}
	}
	return nil
}