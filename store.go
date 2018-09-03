package habolt

import (
	"encoding/json"
	"errors"
	"reflect"

	"github.com/boltdb/bolt"
)

const (
	dbFileMode = 0600
)

var (
	dbBucket = []byte("hab_rr")

	// An error indicating a given key does not exist
	ErrKeyNotFound = errors.New("DB Key not found")
)

// Options contains all the configuraiton used to open the BoltDB
type Options struct {
	// Path is the file path to the BoltDB to use
	Path string

	// BoltOptions contains any specific BoltDB options you might
	// want to specify [e.g. open timeout]
	BoltOptions *bolt.Options

	// NoSync causes the database to skip fsync calls after each
	// write to the log. This is unsafe, so it should be used
	// with caution.
	NoSync bool
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
}

// New uses the supplied options to open the BoltDB and prepare it for use as a raft backend.
func NewStore(options Options) (*Store, error) {
	// Try to connect
	handle, err := bolt.Open(options.Path, dbFileMode, options.BoltOptions)
	if err != nil {
		return nil, err
	}
	handle.NoSync = options.NoSync

	// Create the new store
	store := &Store{
		conn: handle,
		path: options.Path,
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

	if _, err := tx.CreateBucketIfNotExists(dbBucket); err != nil {
		return err
	}

	return tx.Commit()
}

// Close is used to gracefully close the DB connection.
func (s *Store) Close() error {
	return s.conn.Close()
}

func (s *Store) List(values interface{}) error {
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

	curs := tx.Bucket(dbBucket).Cursor()
	for key, val := curs.First(); key != nil; key, val = curs.Next() {
		value := reflect.New( slice.Type().Elem() )
		if err := json.Unmarshal(val, value.Interface()); err != nil {
			return err
		}
    	slice.Set( reflect.Append( slice,  value.Elem() ) )
	}

	return tx.Commit()
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

	bucket := tx.Bucket(dbBucket)
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
	
	bucket := tx.Bucket(dbBucket)
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

	bucket := tx.Bucket(dbBucket)
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