package couchbase

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/couchbase/gocb"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

const (
	dbname = "db14"
	couchbaseIndexs	 = "couchbase.indexs"
	GlobalTimeout = 1 *time.Hour
)

type couchbaseDB struct {
	cli *gocb.Cluster
	database *gocb.Bucket

	indexs []string
	shouldDropIndex bool
	shouldDropDatabase bool

	keyCount  int64
	fieldCount int64
}

func (c *couchbaseDB) Close() error {
	//err := c.cli.Manager("user", "password").RemoveBucket(dbname)
	//if err != nil {
	//	panic(err)
	//}
	return nil
}
func (c *couchbaseDB) Delete(ctx context.Context, table string, key string) error {
	return nil
}
func (c *couchbaseDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan is not supported")
}

func (c *couchbaseDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	return nil
}

func (c *couchbaseDB) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	return ctx
}

func (c *couchbaseDB) CleanupThread(ctx context.Context) {
	if c.shouldDropIndex && (len(c.indexs) > 0) {
		// 删除所有索引
		start := time.Now()
		mgr := c.database.Manager("", "")
		allIndex, err := mgr.GetIndexes()
		if err != nil {
			fmt.Printf("[ERROR] drop all indexs error: %v\n", err)
		} else {
			for _, index := range allIndex {
				err := c.database.Manager("", "").DropIndex(index.Name, true)
				if err != nil {
					fmt.Printf("[ERROR] drop index '%s' error: %v\n", index.Name, err)
				}
			}
		}
		fmt.Printf("drop all indexs time used: %v\n", time.Now().Sub(start))
	}

	if c.shouldDropDatabase {
		start := time.Now()
		mgr := c.cli.Manager("user", "password")
		err := mgr.RemoveBucket(dbname)
		if err != nil {
			fmt.Printf("[ERROR] drop all database error: %v\n", err)
		}
		err = WatchRemoveBucket(mgr, GlobalTimeout)
		if err != nil {
			fmt.Printf("[ERROR] watch remove bucket error: %v\n", err)
		}
		fmt.Printf("drop all databases time used: %v\n", time.Now().Sub(start))
	}
}

func (c *couchbaseDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var doc map[string][]byte
	_, err := c.database.Get(key, &doc)
	if err != nil {
		fmt.Printf("[ERROR] failed to read couchbase, key = %v, err: %v\n", key, err)
		return nil, err
	}
	return doc, nil
}

func (c *couchbaseDB) ScanValue(ctx context.Context, table string, count int, values map[string][]byte) ([]map[string][]byte, error) {
	// 1. 随机获取某个主键对应的document
	ranKey := c.getRandomKey()
	var doc map[string][]byte
	_, err := c.database.Get(ranKey, &doc)
	if err != nil {
		fmt.Printf("[ERROR] failed to read couchbase, key = %v, err: %v\n", ranKey, err)
		return nil, err
	}

	// 2. 随机获取这个document里某个字段的值
	ranFieldName := c.getRandomField()
	val := doc[ranFieldName]

	start := time.Now()
	fieldstring := ranFieldName + "=\"" + base64.StdEncoding.EncodeToString(val) + "\""

	myQuery := "SELECT * FROM `" + dbname + "` WHERE " + fieldstring
	myN1qlQuery := gocb.NewN1qlQuery(myQuery)
	myN1qlQuery.Timeout(GlobalTimeout)
	rows, err := c.database.ExecuteN1qlQuery(myN1qlQuery, nil)
	if err != nil {
		fmt.Printf("[ERROR] failed to scanvalue couchbase, err: %v\n", err)
		return nil, err
	}
	defer rows.Close()
	var res []map[string][]byte
	var row map[string][]byte
	for rows.Next(&row) {
		res = append(res, row)
	}

	//fmt.Println(myQuery)
	//fmt.Println(res)

	fmt.Printf("==== scan value time used %v\n", time.Now().Sub(start))
	return res, nil
}

// Insert a document.
func (c *couchbaseDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	_, err := c.database.Insert(key,  values, 0)
	if err != nil {
		fmt.Printf("[ERROR] failed to insert couchbase, key = %v, err: %v\n", key, err)
		return err
	}
	return nil
}

func (c *couchbaseDB) getRandomKey() string {
	ran := rand.Int63n(c.keyCount)
	return "user"+strconv.FormatInt(ran, 10)
}

func (c *couchbaseDB) getRandomField() string {
	ran := rand.Int63n(c.fieldCount)
	return "field"+strconv.FormatInt(ran, 10)
}

type couchbaseCreator struct {
}

func (c couchbaseCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	cli, err := gocb.Connect("http://127.0.0.1:8091/")
	if err != nil {
		fmt.Printf("[ERROR] failed to connect db, err: %v\n", err)
		return nil, err
	}

	err = cli.Authenticate(gocb.PasswordAuthenticator{
		Username: "user",
		Password: "password",
	})
	if err != nil {
		fmt.Printf("[ERROR] failed to authenticate db, err: %v\n", err)
		return nil, err
	}



	bu, err := cli.OpenBucket(dbname, "")
	if err != nil {
		fmt.Printf("[WARN] failed to open bucket, err: %v\n", err)
	}

	if bu == nil {
		mgr := cli.Manager("user", "password")
		err := mgr.InsertBucket(&gocb.BucketSettings{
			FlushEnabled:  false,
			IndexReplicas: false,
			Name:          dbname,
			Password:      "",
			Quota:         p.GetInt(prop.CouchbaseMemory, prop.CouchbaseMemoryDefault),
			Replicas:      0,
			Type:          0,
		})
		if err != nil {
			fmt.Printf("[ERROR] failed to insert bucket, err: %v\n", err)
			return nil, err
		}
		time.Sleep(5 * time.Second)
		bu, err = cli.OpenBucket(dbname, "")
		if err != nil {
			fmt.Printf("[ERROR] failed to open bucket, err: %v\n", err)
			return nil, err
		}
	}

	bu.SetOperationTimeout(GlobalTimeout)

	cou := &couchbaseDB{
		cli: cli,
		database: bu,
		shouldDropIndex:    p.GetBool(prop.DropIndex, prop.DropIndexDefault),
		shouldDropDatabase: p.GetBool(prop.DropDatabase, prop.DropDatabaseDefault),
		keyCount:           p.GetInt64(prop.RecordCount, prop.RecordCountDefault) + p.GetInt64(prop.OperationCount, int64(0)),
		fieldCount:         p.GetInt64(prop.FieldCount, 5),
	}

	cou.indexs = getAllField(p.GetString(couchbaseIndexs, ""))
	mgr := bu.Manager("", "")
	if len(cou.indexs) > 0 {
		fmt.Println("create index ....")
		fmt.Printf("indexs = %v\n", cou.indexs)
		start := time.Now()

		for _, fn := range cou.indexs {
			err = mgr.CreateIndex(fn, []string{fn}, true, false)
			if err != nil {
				fmt.Printf("[ERROR] create index error, err: %v\n", err)
			}
		}

		indexs, err := mgr.GetIndexes()
		fmt.Printf("indexs: %+v\n", indexs)
		fmt.Println("start to watch")
		err = WatchBuildingIndexes(mgr, GlobalTimeout)
		//building, err := mgr.BuildDeferredIndexes()
		//fmt.Println("building:", building)
		//indexs, err := mgr.GetIndexes()
		//if err != nil {
		//	fmt.Println("get indexs err:", err)
		//}
		//fmt.Printf("indexs: %+v\n", indexs)
		//err = mgr.WatchIndexes(building, false, 3600 *time.Second)
		if err != nil {
			fmt.Printf("watch index out of time, err: %v\n", err)
			return nil, nil
		}
		fmt.Printf("Create index time used: %v\n", time.Now().Sub(start))
	}
	fmt.Printf("indexes: %+v\n", mgr.GetIndexes())
	return cou, nil
}

func init() {
	ycsb.RegisterDBCreator("couchbase", couchbaseCreator{})
}

func getAllField(str string) []string {
	fields := make([]string, 0)
	if str == "" {
		return fields
	}
	val := strings.TrimSpace(str)
	fields = strings.Split(val, ",")
	return fields
}
func WatchBuildingIndexes(bm *gocb.BucketManager, timeout time.Duration) error {

	curInterval := 50 * time.Millisecond
	timeoutTime := time.Now().Add(timeout)
	for {
		indexes, err := bm.GetIndexes()
		if err != nil {
			return err
		}

		finish := true
		for _, index := range indexes {
			if index.State != "online" {
				finish = false
				break
			}
		}
		if finish {
			break
		}

		curInterval += 5 * time.Second
		if curInterval > 1000 {
			curInterval = 1000
		}

		if time.Now().Add(curInterval).After(timeoutTime) {
			return errors.New("create index time out")
		}

		// Wait till our next poll interval
		time.Sleep(curInterval)
	}

	return nil
}

func WatchRemoveBucket(mgr *gocb.ClusterManager, timeout time.Duration) error {
	curInterval := 50 * time.Millisecond
	timeoutTime := time.Now().Add(timeout)
	for {
		bs, err := mgr.GetBuckets()
		// fmt.Printf("indexes: %+v\n", indexes)
		if err != nil {
			return err
		}

		if len(bs) == 0 {
			break
		}

		curInterval += 5 * time.Second
		if curInterval > 1000 {
			curInterval = 1000
		}

		if time.Now().Add(curInterval).After(timeoutTime) {
			return errors.New("create index time out")
		}

		// Wait till our next poll interval
		time.Sleep(curInterval)
	}

	return nil
}

