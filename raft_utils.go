package habolt

import (
	"crypto/md5"
	"fmt"
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
	raftAddr := has.RaftAddr()

	if raftStore, raftSnaps, err = has.raftStores(); err != nil {
		return
	}
	if raftTrans, err = has.raftTransport(); err != nil {
		return
	}
	
	raftConf.LocalID = raft.ServerID(raftAddr)
	raftConf.LogOutput = has.LogOutput

	has.raftServer, err = raft.NewRaft(raftConf, (*fsm)(has), raftStore, raftStore, raftSnaps, raftTrans)
	return
}

func (has *HaStore) raftStores() (store *raftboltdb.BoltStore, snapshot *raft.FileSnapshotStore, err error) {
	var db_path string = "/tmp"
	// if db_path, err = os.Getwd(); err != nil {
	// 	return
	// }
	raft_id := fmt.Sprintf("%x", md5.Sum([]byte( has.RaftAddr() )))

	db_path = filepath.Join(db_path, raft_id)
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
	snapshot, err = raft.NewFileSnapshotStore(db_path, retainSnapshotCount, os.Stderr)
	return
}

func (has *HaStore) raftTransport() (transport *raft.NetworkTransport, err error) {
	var tcpAddr *net.TCPAddr
	raftAddr := has.RaftAddr()
	if tcpAddr, err = net.ResolveTCPAddr("tcp", raftAddr); err != nil {
		return
	}
	transport, err = raft.NewTCPTransport(raftAddr, tcpAddr, 3, 10*time.Second, os.Stderr)
	return
}

func (has *HaStore) raftBootstrap(peers ...string) error {
	raftAddr := has.RaftAddr()
	bootstrapConfig := raft.Configuration{
		Servers: []raft.Server{
			{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(raftAddr),
				Address:  raft.ServerAddress(raftAddr),
			},
		},
	}

	// Add known peers to bootstrap
	for _, node := range peers {
		if node != raftAddr {
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