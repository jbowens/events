package events

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"os"

	"github.com/boltdb/bolt"
)

func Open(path string) (*Logger, error) {
	db, err := bolt.Open(path, os.ModePerm, nil)
	if err != nil {
		return nil, err
	}

	return &Logger{
		db:   db,
		path: path,
	}, nil
}

type Logger struct {
	db   *bolt.DB
	path string
}

func (l *Logger) Log(typ string, event interface{}) error {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(event)
	if err != nil {
		return err
	}

	return l.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(typ))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		id, _ := b.NextSequence()
		return b.Put(itob(id), buf.Bytes())
	})
}

func (l *Logger) Iter(typ string, fn func(decodeFn func(interface{}) error) error) error {
	return l.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(typ))
		c := b.Cursor()

		var buf bytes.Buffer
		dec := gob.NewDecoder(&buf)
		decFn := func(event interface{}) error {
			return dec.Decode(event)
		}

		for k, v := c.First(); k != nil; k, v = c.Next() {
			buf.Reset()
			buf.Write(v)

			err := fn(decFn)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (l *Logger) Close() error {
	return l.db.Close()
}

// itob returns an 8-byte big endian representation of v.
func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
