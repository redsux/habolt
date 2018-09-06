package habolt

import (
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

func (has *HaStore) initRaft() (err error) {
	var (
		raftStore *raftboltdb.BoltStore
		raftSnaps *raft.FileSnapshotStore
		raftTrans *raft.NetworkTransport
		raftConf  *raft.Config = raft.DefaultConfig()
	)

	if raftStore, raftSnaps, err = has.raftStores(); err != nil {
		return
	}
	if raftTrans, err = has.raftTransport(); err != nil {
		return
	}
	
	raftConf.LocalID = has.realAddr().Raft().raftID()
	raftConf.Logger = has.store.logger

	has.raftServer, err = raft.NewRaft(raftConf, (*fsm)(has), raftStore, raftStore, raftSnaps, raftTrans)
	return
}

func (has *HaStore) raftStores() (store *raftboltdb.BoltStore, snapshot *raft.FileSnapshotStore, err error) {
	var db_path string = "/tmp"
	db_path = filepath.Join(db_path, has.realAddr().Raft().Md5())
	if err = os.RemoveAll(db_path + "/"); err != nil {
		return
	}
	if err = os.MkdirAll(db_path, 0777); err != nil {
		return
	}
	db_file := filepath.Join(db_path, "raft.db")
	if store, err = raftboltdb.NewBoltStore(db_file); err != nil {
		return
	}
	snapshot, err = raft.NewFileSnapshotStoreWithLogger(db_path, retainSnapshotCount, has.store.logger)
	return
}

func (has *HaStore) raftTransport() (transport *raft.NetworkTransport, err error) {
	var tcpAddr *net.TCPAddr
	if tcpAddr, err = net.ResolveTCPAddr("tcp", has.realAddr().Raft().String() ); err != nil {
		return
	}
	transport, err = raft.NewTCPTransportWithLogger(has.Bind.Raft().String(), tcpAddr, 3, 10*time.Second, has.store.logger)
	return
}

func (has *HaStore) raftBootstrap(peers ...string) error {
	addr := has.realAddr().Raft()
	bootstrapConfig := raft.Configuration{
		Servers: []raft.Server{
			{
				Suffrage: raft.Voter,
				ID:       addr.raftID(),
				Address:  addr.raftAddress(),
			},
		},
	}

	for _, node := range peers {
		if node != addr.String() {
			bootstrapConfig.Servers = append(bootstrapConfig.Servers, raft.Server{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(node),
				Address:  raft.ServerAddress(node),
			})
		}
	}

	future := has.raftServer.BootstrapCluster(bootstrapConfig)
	return future.Error()
}