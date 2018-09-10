package habolt

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"path/filepath"
	"reflect"

	"github.com/boltdb/bolt"
	"github.com/hashicorp/go-sockaddr"
)

// StaticStore is a wrapper of BoltDB which implements our Store interface
type StaticStore struct {
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

	// Bind IP
	bindIP  net.IP
}

// NewStaticStore uses the supplied options to open the BoltDB and prepare it for use as a raft backend.
func NewStaticStore(options *Options) (*StaticStore, error) {
	if !options.isValid() {
		return nil, ErrMissingPath
	}
	// Try to connect
	handle, err := bolt.Open(options.Path, options.FileMode, options.BoltOptions)
	if err != nil {
		return nil, err
	}
	handle.NoSync = options.NoSync

	// Create the new StaticStore
	StaticStore := &StaticStore{
		conn:   handle,
		path:   options.Path,
		bucket: []byte(options.Bucket),
		output: options.LogOutput,
		logger: options.Logger,
	}

	// If the StaticStore was opened read-only, don't try and create buckets
	if !options.readOnly() {
		// Set up our buckets
		if err := StaticStore.initialize(); err != nil {
			StaticStore.Close()
			return nil, err
		}
	}
	return StaticStore, nil
}

// initialize is used to set up all of the buckets.
func (s *StaticStore) initialize() error {
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
func (s *StaticStore) Close() error {
	return s.conn.Close()
}

// Logger return the log.Logger object
func (s *StaticStore) Logger() *log.Logger {
	return s.logger
}

// LogLevel change the level of logs, only available with our HabOuput
func (s *StaticStore) LogLevel(level int) {
	if out, ok := s.output.(*HaOutput); ok {
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

// ListRaw retrive all "key"/"value" with any modification
func (s *StaticStore) ListRaw() (map[string]string, error) {
	tx, err := s.conn.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	res := make(map[string]string)
	curs := tx.Bucket(s.bucket).Cursor()
	for key, val := curs.First(); key != nil; key, val = curs.Next() {
		res[string(key)] = string(val)
	}

	return res, nil
}

// List retreive all values in our BoltDB, evertyhing will be "unmarshal" thanks a JSON format
// BoltDB keys could be filtering thanks wildcard patterns
func (s *StaticStore) List(values interface{}, patterns ...string) error {
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
			value := reflect.New(slice.Type().Elem())
			if err := json.Unmarshal(val, value.Interface()); err != nil {
				return err
			}
			slice.Set(reflect.Append(slice, value.Elem()))
		}
	}

	return nil
}

// Get retreive the value associated to the "key" in BoltDB and "json.Unmashal" it to "value"
func (s *StaticStore) Get(key string, value interface{}) error {
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
	val := bucket.Get([]byte(key))

	if val == nil {
		return ErrKeyNotFound
	}
	return json.Unmarshal(val, value)
}

// Set "json.Mashal" the "value" and store it in BoltDB with the specified "key"
func (s *StaticStore) Set(key string, value interface{}) error {
	val, err := json.Marshal(value)
	if err != nil {
		return err
	}
	tx, err := s.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(s.bucket)
	if err := bucket.Put([]byte(key), val); err != nil {
		return err
	}

	return tx.Commit()
}

// Delete removes the "key" in BoltDB
func (s *StaticStore) Delete(key string) error {
	tx, err := s.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(s.bucket)
	if err := bucket.Delete([]byte(key)); err != nil {
		return err
	}
	return tx.Commit()
}

// Addresses return slice which contains a signe entry : "GetPrivateIP" from go-sockaddr
func (s *StaticStore) Addresses() ([]HaAddress, error) {
	var (
		ip string
		err error
	)
	if s.bindIP != nil {
		ip = string(s.bindIP)
	} else {
		if ip, err = sockaddr.GetPrivateIP(); err != nil {
			return nil, err
		}
	}
	s.logger.Printf("[DEBUG] StaticStore.Addresses = %s", ip)
	return []HaAddress{HaAddress{Address: ip}}, nil
}

// Sync performs an fsync on the database file handle. This is not necessary
// under normal operation unless NoSync is enabled, in which this forces the
// database file to sync against the disk.
func (s *StaticStore) Sync() error {
	return s.conn.Sync()
}

// BindTo change the behavior of "Addresses" function to return another IP address
func (s *StaticStore) BindTo(ip string) {
	if addr := net.ParseIP(ip); addr != nil {
		s.bindIP = addr
	}
}