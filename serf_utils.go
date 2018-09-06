package habolt

import (
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
)

func (has *HaStore) initSerf() (err error) {
	has.serfEvents = make(chan serf.Event, 16)

	memberlistConfig := memberlist.DefaultWANConfig()
	memberlistConfig.BindAddr = has.Bind.Address
	memberlistConfig.BindPort = int(has.Bind.Port)
	memberlistConfig.AdvertiseAddr = has.realAddr().Address
	memberlistConfig.AdvertisePort = int(has.realAddr().Port)
	memberlistConfig.Logger = has.store.logger

	serfConfig := serf.DefaultConfig()
	serfConfig.NodeName = has.realAddr().String()
	serfConfig.EventCh = has.serfEvents
	serfConfig.MemberlistConfig = memberlistConfig
	serfConfig.Logger = has.store.logger

	has.serfServer, err = serf.Create(serfConfig)
	return
}

func (has *HaStore) serfMemberListener(evt serf.MemberEvent) error {
	for _, member := range evt.Members {
		changedPeer := serfMemberToListen(member).Raft()
		
		var action raft.Future

		switch evt.EventType() {
		case serf.EventMemberJoin:
			action = has.raftServer.AddVoter(changedPeer.raftID(), changedPeer.raftAddress(), 0, 0)
		case serf.EventMemberFailed:
			fallthrough
		case serf.EventMemberReap:
			fallthrough
		case serf.EventMemberLeave:
			action = has.raftServer.RemoveServer(changedPeer.raftID(), 0, 0)
		}

		if action != nil {
			if err := action.Error(); err != nil {
				return err
			}
		}
	}
	return nil
}