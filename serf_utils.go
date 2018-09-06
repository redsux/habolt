package habolt

import (
	"fmt"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
)

func (has *HaStore) initSerf() (err error) {
	has.serfEvents = make(chan serf.Event, 16)

	memberlistConfig := memberlist.DefaultWANConfig()
	//memberlistConfig.BindAddr = has.Address // default "0.0.0.0"
	memberlistConfig.BindPort = has.Port
	memberlistConfig.AdvertiseAddr = has.advAddr()
	memberlistConfig.AdvertisePort = has.Port
	memberlistConfig.Logger = has.store.logger

	serfConfig := serf.DefaultConfig()
	serfConfig.NodeName = has.SerfAddr()
	serfConfig.EventCh = has.serfEvents
	serfConfig.MemberlistConfig = memberlistConfig
	serfConfig.Logger = has.store.logger

	has.serfServer, err = serf.Create(serfConfig)
	return
}

func (has *HaStore) serfMemberListener(evt serf.MemberEvent) error {
	for _, member := range evt.Members {
		changedPeer := fmt.Sprintf("%s:%d", member.Addr.String(), member.Port + 1)
		peerId := raft.ServerID(changedPeer)
		peerAddr := raft.ServerAddress(changedPeer)

		var action raft.Future

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