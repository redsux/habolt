package habolt

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/fatih/color"
)

const (
	// DEBUG log level
	DEBUG = 0
	// INFO log level
	INFO = 1
	// WARNING log level
	WARNING = 2
	// ERROR log level
	ERROR = 3
)

var (
	funcPrints = map[string]interface{}{
		"[DEBUG]": color.Cyan,
		"[ERR]":   color.Red,
		"[ERROR]": color.Red,
		"[WARN]":  color.Yellow,
		"[INFO]":  color.Green,
	}
	levelFilters = map[string]int{
		"[DEBUG]": DEBUG,
		"[ERR]":   ERROR,
		"[ERROR]": ERROR,
		"[WARN]":  WARNING,
		"[INFO]":  INFO,
	}
)

// HaOutput implements "io.Writer" interface to filter output content.
type HaOutput struct {
	level int
}

// NewOutput create a new HaOutput with the log level defined in parameter
func NewOutput(l int) *HaOutput {
	return &HaOutput{level: l}
}

// NewOutputStr create a new HaOutput thanks a log level string ("debug", "warning", "error", "info")
func NewOutputStr(s string) (*HaOutput, error) {
	for txt, lvl := range levelFilters {
		if strings.EqualFold(s, strings.Trim(txt, "[]")) {
			return NewOutput(lvl), nil
		}
	}
	return nil, fmt.Errorf("Log level %s not found", s)
}

// Level change the current log level with value in parameter
func (hao *HaOutput) Level(l int) {
	hao.level = l
}

// Write filter the parameter and output it if needed
func (hao *HaOutput) Write(p []byte) (n int, err error) {
	for k, v := range levelFilters {
		if bytes.Contains(p, []byte(k)) {
			if hao.level > v {
				return len(p), nil
			}
		}
	}
	for k, v := range funcPrints {
		if bytes.Contains(p, []byte(k)) {
			v.(func(string, ...interface{}))("%s", string(p[:len(p)-1]))
			return len(p), nil
		}
	}
	return 0, errors.New("No level found ?")
}
