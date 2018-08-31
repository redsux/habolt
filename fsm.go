package habolt

import (
	"io"
	"github.com/fatih/color"
	"github.com/hashicorp/raft"
)

type fsm struct {
}

func (f *fsm) Apply(l *raft.Log) interface{} {
	color.Green( "[INFO] Apply %s", string(l.Data) )
	return nil
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (f *fsm) Restore(io.ReadCloser) error {
	return nil
}