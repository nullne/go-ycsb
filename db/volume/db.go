package volume

import (
	"context"
	"errors"
	"os"
	"strings"

	"github.com/magiconair/properties"
	"github.com/minio/minio/pkg/volume"
	"github.com/minio/minio/pkg/volume/index/badger"
	"github.com/minio/minio/pkg/volume/index/rocksdb"
	"github.com/minio/minio/pkg/volume/interfaces"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const (
	volumeIndexRoot = "volume.index-root"
	volumeIndexDB   = "volume.index-db"
)

type volumeCreator struct{}

func (c volumeCreator) Create(p *properties.Properties) (ycsb.DB, error) {

	dt := p.GetString(volumeIndexDB, "badger")
	indexRoot := p.GetString(volumeIndexRoot, "")

	table := p.GetString(prop.TableName, "/tmp/volume-test")

	if p.GetBool(prop.DropData, prop.DropDataDefault) {
		if err := os.RemoveAll(table); err != nil {
			return nil, err
		}
		if indexRoot != "" && indexRoot != "/" {
			if err := os.RemoveAll(indexRoot); err != nil {
				return nil, err
			}
		}
	}

	volumes := volume.NewVolumes(newVolumeWrapper(dt, indexRoot))

	// prepare the volume at the first
	if err := volumes.Add(context.Background(), table); err != nil {
		return nil, err
	}
	// create the bucket if not existed
	// client.MakeBucket()
	return &volumeDB{
		db: volumes,
	}, nil
}

func newVolumeWrapper(db, indexRoot string) func(ctx context.Context, dir string) (interfaces.Volume, error) {
	return func(ctx context.Context, dir string) (interfaces.Volume, error) {
		var err error
		if dir, err = volume.GetValidPath(dir); err != nil {
			return nil, err
		}

		if err := volume.MkdirIfNotExist(dir); err != nil {
			return nil, err
		}

		options := volume.IndexOptions{}
		options.Root = indexRoot

		var idx volume.Index
		switch db {
		case "rocksdb":
			idx, err = rocksdb.NewIndex(dir, options)
		case "badger":
			idx, err = badger.NewIndex(dir, options)
		default:
			panic(errors.New("invalid index"))
		}
		if err != nil {
			return nil, err
		}
		v, err := volume.NewVolume(ctx, dir, idx)
		if err != nil {
			// idx.Close()
			return nil, err
		}
		v.SetDirectIndexSaving(func(key string) bool {
			return strings.HasSuffix(key, "xl.json")
		})
		return v, nil
	}
}

type volumeDB struct {
	db *volume.Volumes
}

// Close closes the database layer.
func (db *volumeDB) Close() error {
	return nil
}

// InitThread initializes the state associated to the goroutine worker.
// The Returned context will be passed to the following usage.
func (db *volumeDB) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	return ctx
}

// CleanupThread cleans up the state when the worker finished.
func (db *volumeDB) CleanupThread(ctx context.Context) {
}

// Read reads a record from the database and returns a map of each field/value pair.
// table: The name of the table.
// key: The record key of the record to read.
// fields: The list of fields to read, nil|empty for reading all.
func (db *volumeDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	vol, err := db.db.Get(table)
	if err != nil {
		return nil, err
	}
	bs, err := vol.ReadAll(key)
	if err != nil {
		return nil, err
	}
	return map[string][]byte{"field0": bs}, nil
}

// Scan scans records from the database.
// table: The name of the table.
// startKey: The first record key to read.
// count: The number of records to read.
// fields: The list of fields to read, nil|empty for reading all.
func (db *volumeDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	vol, err := db.db.Get(table)
	if err != nil {
		return nil, err
	}
	entries, err := vol.List(startKey, "", count)
	if err != nil {
		return nil, err
	}
	res := make([]map[string][]byte, count)
	for i, e := range entries {
		res[i] = map[string][]byte{e: nil}
	}
	return res, nil
}

// Update updates a record in the database. Any field/value pairs will be written into the
// database or overwritten the existing values with the same field name.
// table: The name of the table.
// key: The record key of the record to update.
// values: A map of field/value pairs to update in the record.
func (db *volumeDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	vol, err := db.db.Get(table)
	if err != nil {
		return err
	}

	var bs []byte
	for _, v := range values {
		bs = v
		break
	}
	return vol.WriteAll(key, bs)
}

// Insert inserts a record in the database. Any field/value pairs will be written into the
// database.
// table: The name of the table.
// key: The record key of the record to insert.
// values: A map of field/value pairs to insert in the record.
func (db *volumeDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	return db.Update(ctx, table, key, values)
}

// Delete deletes a record from the database.
// table: The name of the table.
// key: The record key of the record to delete.
func (db *volumeDB) Delete(ctx context.Context, table string, key string) error {
	vol, err := db.db.Get(table)
	if err != nil {
		return err
	}
	return vol.Delete(key)
}

func init() {
	ycsb.RegisterDBCreator("volume", volumeCreator{})
}
