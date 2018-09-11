package habolt

import (
	"crypto/md5"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
)

// HaAddress is a helper struct to define our listening
// ip:port addresses between Serf & Raft
type HaAddress struct {
	Address string
	Port    uint16
}

// NewListen create a new HaAddress thanks an "ip:port" string
func NewListen(listen string) (*HaAddress, error) {
	var (
		ips  []net.IP
		host string
		port uint64
		err  error
	)
	if !strings.ContainsAny(listen, ":") {
		host = listen
		port = 0
	} else {
		var strPort string
		if host, strPort, err = net.SplitHostPort(listen); err != nil {
			return nil, err
		}
		if port, err = strconv.ParseUint(strPort, 10, 16); err != nil {
			return nil, err
		}
	}
	if host == "" {
		return &HaAddress{
			Port:    uint16(port),
		}, nil
	}
	if ips, err = net.LookupIP(host); err != nil {
		return nil, err
	}
	for _, ip := range ips {
		return &HaAddress{
			Address: ip.String(),
			Port:    uint16(port),
		}, nil
	}
	return nil, fmt.Errorf("Host %s not valid", host)
}

// NewAddress create a new HaAddress with an ip string and a port int
func NewAddress(addr string, port int) *HaAddress {
	return &HaAddress{
		Address: addr,
		Port:    uint16(port),
	}
}

func serfMemberToListen(member serf.Member) *HaAddress {
	return &HaAddress{
		Address: member.Addr.String(),
		Port:    member.Port,
	}
}

func (hal *HaAddress) String() string {
	return fmt.Sprintf("%s:%d", hal.Address, hal.Port)
}

// Md5 return a MD5 hash of the string "Address:Port"
func (hal *HaAddress) Md5() string {
	return fmt.Sprintf("%x", md5.Sum([]byte(hal.String())))
}

// Raft convert our HaAddress for Raft (Port + 1)
func (hal *HaAddress) Raft() *HaAddress {
	return &HaAddress{
		Address: hal.Address,
		Port:    hal.Port + 1,
	}
}

func (hal *HaAddress) raftID() raft.ServerID {
	return raft.ServerID(hal.String())
}

func (hal *HaAddress) raftAddress() raft.ServerAddress {
	return raft.ServerAddress(hal.String())
}