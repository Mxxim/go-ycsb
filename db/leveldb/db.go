package leveldb

import (
	"context"
	"fmt"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	ldb "github.com/pingcap/goleveldb/leveldb"
	"github.com/pingcap/goleveldb/leveldb/opt"
	ldbutil "github.com/pingcap/goleveldb/leveldb/util"
)

/**
 * currently, set the field count=1, length=300
 * the value is mapped to a table structure, so the value can be parsed as map["field0"]= "a byte slice"
 */

const(
	path        = "leveldb.path"
	defaultPath = "data/ldb/"
)

func init() {
	ycsb.RegisterDBCreator("leveldb", leveldbCreator{})
}

type LDBInstance struct {
	leveldb *ldb.DB
	r       *util.RowCodec
	bufPool *util.BufPool
	readOpts *opt.ReadOptions
	writeOpts *opt.WriteOptions
}

type leveldbCreator struct {}

func (leveldbCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	l, err := ldb.OpenFile(p.GetString(path, defaultPath), &opt.Options{})
	if err != nil {
		return nil, err
	}
	instance := &LDBInstance{
		leveldb: l,
		bufPool: util.NewBufPool(),
		r:       util.NewRowCodec(p),
		readOpts: &opt.ReadOptions{},
		writeOpts: &opt.WriteOptions{},
	}
	return instance, nil
}

func (db *LDBInstance) Close() error {
	return db.leveldb.Close()
}

func (db *LDBInstance) InitThread(ctx context.Context, _ int, _ int) context.Context { return ctx }

func (db *LDBInstance) CleanupThread(ctx context.Context) {}

func (db *LDBInstance) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	value, err := db.leveldb.Get(db.getRowKey(table, key), db.readOpts)
	if err != nil {
		return nil, err
	}
	return db.r.Decode(value, fields)
}

func (db *LDBInstance) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	fmt.Printf("=== leveldb scan, startKey = %s, count = %v, len(fields) = %v\n", startKey, count, len(fields))
	res := make([]map[string][]byte, count)
	rowStartKey := db.getRowKey(table, startKey)
	it := db.leveldb.NewIterator(&ldbutil.Range{Start: rowStartKey}, nil)
	defer it.Release()

	i := 0
	for it = it; it.Valid() && i < count; it.Next() {
		value := it.Value()
		m, err := db.r.Decode(value, fields)
		if err != nil {
			return nil, err
		}
		res[i] = m
		i++
	}

	if err := it.Error(); err != nil {
		return nil, err
	}

	return res, nil
}

func (db *LDBInstance) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	m, err := db.Read(ctx, table, key, nil)
	if err != nil {
		return err
	}

	for field, value := range values {
		m[field] = value
	}

	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	rowData, err := db.r.Encode(buf.Bytes(), m)
	if err != nil {
		return err
	}

	rowKey := db.getRowKey(table, key)

	return db.leveldb.Put(rowKey, rowData, db.writeOpts)
}

func (db *LDBInstance) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {

	rowKey := db.getRowKey(table, key)

	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	rowData, err := db.r.Encode(buf.Bytes(), values)
	if err != nil {
		return err
	}

	return db.leveldb.Put(rowKey, rowData, db.writeOpts) //todo
}

func (db *LDBInstance) Delete(ctx context.Context, table string, key string) error { return nil }

func (db *LDBInstance) ScanValue(ctx context.Context, table string, count int, values map[string][]byte) ([]map[string][]byte, error) {
	return nil, nil
}

func (db *LDBInstance) getRowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s-%s", table, key))
}