package habolt

import (
	"crypto/md5"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
)

type HaListen struct {
	Address string
	Port    uint16
}

func NewListen(addr string, port int) *HaListen {
	return &HaListen{
		Address: addr,
		Port: uint16(port),
	}
}

func serfMemberToListen(member serf.Member) *HaListen {
	return &HaListen{
		Address: member.Addr.String(),
		Port: member.Port,
	}
}

func (hal *HaListen) String() string {
	return fmt.Sprintf("%s:%d", hal.Address, hal.Port)
}

func (hal *HaListen) Md5() string {
	return fmt.Sprintf("%x", md5.Sum( []byte( hal.String() ) ) )
}

func (hal *HaListen) Raft() *HaListen {
	return &HaListen{
		Address: hal.Address,
		Port: hal.Port + 1,
	}
}

func (hal *HaListen) raftID() raft.ServerID {
	return raft.ServerID( hal.String() )
}

func (hal *HaListen) raftAddress() raft.ServerAddress {
	return raft.ServerAddress( hal.String() )
}