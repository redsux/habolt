package habolt

import (
	"errors"
	"io"
	"log"
	"os"

	"github.com/boltdb/bolt"
)

var (
	// ErrMissingPath for a wrong configuration
	ErrMissingPath = errors.New("NewStaticStore missing Path")
	// ErrKeyNotFound for a given key which does not exist
	ErrKeyNotFound = errors.New("DB Key not found")
)

// Store interface to define useful functions
type Store interface {
	Close() error
	ListRaw() (map[string]string, error)
	List(interface{}, ...string) error
	Get(string, interface{}) error
	Set(string, interface{}) error
	Delete(string) error
	Logger() *log.Logger
	LogLevel(int)
}

// Options contains all the configuraiton used to open the BoltDB
type Options struct {
	// Path is the file path to the BoltDB to use
	Path string

	// BoltDB file mode
	FileMode os.FileMode

	// Bucket to create at the beginning
	Bucket string

	// BoltOptions contains any specific BoltDB options you might
	// want to specify [e.g. open timeout]
	BoltOptions *bolt.Options

	// NoSync causes the database to skip fsync calls after each
	// write to the log. This is unsafe, so it should be used
	// with caution.
	NoSync bool

	// Where our default Logger will output logs
	LogOutput io.Writer

	// Logger to use everywhere, if nil a new log.Logger will be defined with LogOutput
	Logger *log.Logger
}

func (o *Options) isValid() bool {
	if o.FileMode == 0 {
		o.FileMode = 0600
	}
	if o.Bucket == "" {
		o.Bucket = "default"
	}
	if o.LogOutput == nil {
		o.LogOutput = NewOutput(INFO)
	}
	if o.Logger == nil {
		o.Logger = log.New(o.LogOutput, "", log.LstdFlags)
	}
	return o.Path != ""
}

// readOnly returns true if the contained bolt options say to open
// the DB in readOnly mode
func (o *Options) readOnly() bool {
	return o != nil && o.BoltOptions != nil && o.BoltOptions.ReadOnly
}
