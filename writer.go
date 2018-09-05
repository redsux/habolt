package habolt

import (
	"bytes"
	"errors"
	"github.com/fatih/color"
)

const (
	LVL_DEBUG = 0
	LVL_INFO = 1
	LVL_WARNING = 2
	LVL_ERROR = 3
)

var (
	func_print = map[string]interface{}{
		"[DEBUG]": color.Cyan,
		"[ERR]": color.Red,
		"[WARN]": color.Yellow,
		"[INFO]": color.Green,
	}
	level_filter = map[string]int{
		"[DEBUG]": LVL_DEBUG,
		"[ERR]": LVL_ERROR,
		"[WARN]": LVL_WARNING,
		"[INFO]": LVL_INFO,
	}
)

type HabOutput struct {
    level int
}

func NewOutput(l int) *HabOutput {
	return &HabOutput{level: l}
}

func (hao *HabOutput) Level(l int) {
	hao.level = l
}

func (hao *HabOutput) Write(p []byte) (n int, err error) {	
	for k, v := range level_filter {
		if bytes.Contains(p, []byte(k)) {
			if hao.level > v {
				return len(p), nil
			}
		}
	}
	for k, v := range func_print {
		if bytes.Contains(p, []byte(k)) {
			v.(func(string, ...interface{}))("%s", string(p[:len(p)-1]))
			return len(p), nil
		}
	}
	return 0, errors.New("No level found ?")
}