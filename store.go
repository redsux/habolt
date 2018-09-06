package habolt

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"

	"github.com/boltdb/bolt"
)

var (
	// Error of wrong configuration
	ErrMissingPath = errors.New("NewStore missing Path.")
	// An error indicating a given key does not exist
	ErrKeyNotFound = errors.New("DB Key not found.")
)

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
		o.LogOutput = NewOutput(LVL_INFO)
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

type Store struct {
	// conn is the underlying handle to the db.
	conn *bolt.DB

	// The path to the Bolt database file
	path string

	// Bucket to use
	bucket []byte

	// Log writer
	output io.Writer

	// Logger
	logger *log.Logger
}

// New uses the supplied options to open the BoltDB and prepare it for use as a raft backend.
func NewStore(options *Options) (*Store, error) {
	if !options.isValid() {
		return nil, ErrMissingPath
	}
	// Try to connect
	handle, err := bolt.Open(options.Path, options.FileMode, options.BoltOptions)
	if err != nil {
		return nil, err
	}
	handle.NoSync = options.NoSync

	// Create the new store
	store := &Store{
		conn: handle,
		path: options.Path,
		bucket: []byte(options.Bucket),
		output: options.LogOutput,
		logger: options.Logger,
	}

	// If the store was opened read-only, don't try and create buckets
	if !options.readOnly() {
		// Set up our buckets
		if err := store.initialize(); err != nil {
			store.Close()
			return nil, err
		}
	}
	return store, nil
}


// initialize is used to set up all of the buckets.
func (s *Store) initialize() error {
	tx, err := s.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.CreateBucketIfNotExists(s.bucket); err != nil {
		return err
	}

	return tx.Commit()
}

// Close is used to gracefully close the DB connection.
func (s *Store) Close() error {
	return s.conn.Close()
}

// Change loglevel, only available with our HabOuput
func (s *Store) LogLevel(level int) {
	if out, ok := s.output.(*HabOutput); ok {
		out.Level(level)
		s.logger.Printf("[DEBUG] Log level changed to %d\n", level)
	}
}

func found(str string, patterns []string) bool {
	if str == "" {
		return false
	}
	if patterns == nil || len(patterns) == 0 {
		return true
	}
	for _, pattern := range patterns {
		if ok, err := filepath.Match(pattern, str); err == nil && ok {
			return true
		}
	}
	return false
}

func (s *Store) List(values interface{}, patterns ...string) error {
	vtype := reflect.TypeOf(values)
    if vtype.Kind() != reflect.Ptr && vtype.Elem().Kind() != reflect.Slice {
        return errors.New("Not a Pointer of Slice")
    }
	slice := reflect.ValueOf(values).Elem()

	tx, err := s.conn.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	curs := tx.Bucket(s.bucket).Cursor()
	for key, val := curs.First(); key != nil; key, val = curs.Next() {
		if found(string(key), patterns) {
			value := reflect.New( slice.Type().Elem() )
			if err := json.Unmarshal(val, value.Interface()); err != nil {
				return err
			}
			slice.Set( reflect.Append( slice,  value.Elem() ) )
		}
	}

	return nil
}

func (s *Store) Get(key string, value interface{}) error {
	vtype := reflect.TypeOf(value)
    if vtype.Kind() != reflect.Ptr {
        return errors.New("Not a Pointer")
    }
	tx, err := s.conn.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(s.bucket)
	val := bucket.Get( []byte(key) )

	if val == nil {
		return ErrKeyNotFound
	}
	return json.Unmarshal(val, value)
}

func (s *Store) Set(key string, value interface{}) error {
	tx, err := s.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	val, err := json.Marshal(value)
	if err != nil {
		return err
	}
	
	bucket := tx.Bucket(s.bucket)
	if err := bucket.Put([]byte(key), val); err != nil {
		return err
	}

	return tx.Commit()
}

func (s *Store) Delete(key string) error {
	tx, err := s.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(s.bucket)
	if err := bucket.Delete( []byte(key) ); err != nil {
		return err
	}
	return tx.Commit()
}

// Sync performs an fsync on the database file handle. This is not necessary
// under normal operation unless NoSync is enabled, in which this forces the
// database file to sync against the disk.
func (s *Store) Sync() error {
	return s.conn.Sync()
}