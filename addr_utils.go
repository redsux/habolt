package habolt

import (
	"crypto/md5"
	"fmt"
	"net"
	"strconv"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
)

type HaAddress struct {
	Address string
	Port    uint16
}

func NewListen(listen string) (*HaAddress, error) {
	addr, strPort, err := net.SplitHostPort(listen)
	if err != nil {
		return nil, err
	}
	uint64Port, err := strconv.ParseUint(strPort, 10, 16)
	if err != nil {
		return nil, err
	}
	return &HaAddress{
		Address: addr,
		Port: uint16(uint64Port),
	}, nil
}

func NewAddress(addr string, port int) *HaAddress {
	return &HaAddress{
		Address: addr,
		Port: uint16(port),
	}
}

func serfMemberToListen(member serf.Member) *HaAddress {
	return &HaAddress{
		Address: member.Addr.String(),
		Port: member.Port,
	}
}

func (hal *HaAddress) String() string {
	return fmt.Sprintf("%s:%d", hal.Address, hal.Port)
}

func (hal *HaAddress) Md5() string {
	return fmt.Sprintf("%x", md5.Sum( []byte( hal.String() ) ) )
}

func (hal *HaAddress) Raft() *HaAddress {
	return &HaAddress{
		Address: hal.Address,
		Port: hal.Port + 1,
	}
}

func (hal *HaAddress) raftID() raft.ServerID {
	return raft.ServerID( hal.String() )
}

func (hal *HaAddress) raftAddress() raft.ServerAddress {
	return raft.ServerAddress( hal.String() )
}