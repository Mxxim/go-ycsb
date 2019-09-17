package couchbase

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/couchbase/gocb"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"strings"
	"time"
)

const (
	dbname = "db13"
	couchbaseIndexs	 = "couchbase.indexs"
	index_name = "test_index"
)

type couchbaseDB struct {
	cli *gocb.Cluster
	database *gocb.Bucket

	hasIndex bool
	indexs []string
	shouldDropIndex bool
}

func (c *couchbaseDB) Close() error {
	//err := c.cli.Manager("user", "password").RemoveBucket(dbname)
	//if err != nil {
	//	panic(err)
	//}
	c.database.Close()
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
}

func (c *couchbaseDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var doc map[string][]byte
	_, err := c.database.Get(key, &doc)
	if err != nil {
		panic(err)
	}
	return doc, nil
}

func (c *couchbaseDB) ScanValue(ctx context.Context, table string, count int, values map[string][]byte) ([]map[string][]byte, error) {
	// limit := int64(count)


	var fieldstring string
	i := 0
	for k, v := range values {
		fieldstring += k
		fieldstring += "=\""
		fieldstring += base64.StdEncoding.EncodeToString(v)
		fieldstring += "\""
		i ++
		if i != len(values) {
			fieldstring += " AND "
		}
	}

	myQuery := "SELECT * FROM `" + dbname + "` WHERE " + fieldstring
	myN1qlQuery := gocb.NewN1qlQuery(myQuery)
	rows, err := c.database.ExecuteN1qlQuery(myN1qlQuery, nil)
	if err != nil {
		panic(err)
	}
	var res []map[string][]byte
	var row map[string][]byte
	for rows.Next(&row) {
		res = append(res, row)
	}

	//fmt.Println(myQuery)
	//fmt.Println(res)



	return res, nil
}

// Insert a document.
func (c *couchbaseDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	_, err := c.database.Insert(key,  values, 0)
	if err != nil {
		panic(err)
	}
	return nil
}

type couchbaseCreator struct {
}

func (c couchbaseCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	cli, _ := gocb.Connect("http://127.0.0.1:8091/")
	_ = cli.Authenticate(gocb.PasswordAuthenticator{
		Username: "user",
		Password: "password",
	})

	//
	//time.Sleep(3*time.Second)


	bu, _ := cli.OpenBucket(dbname, "")
	if bu == nil {
		mgr := cli.Manager("user", "password")
		err := mgr.InsertBucket(&gocb.BucketSettings{
			FlushEnabled:  false,
			IndexReplicas: false,
			Name:          dbname,
			Password:      "",
			Quota:         100,
			Replicas:      0,
			Type:          0,
		})
		if err != nil {
			panic(err)
		}
		time.Sleep(5 * time.Second)
		bu, err = cli.OpenBucket(dbname, "")
		if err != nil {
			panic(err)
		}
	}

	cou := &couchbaseDB{
		cli: cli,
		database: bu,
	}

	hasIndex := p.GetBool(prop.HasIndex, prop.HasIndexDefault)
	if hasIndex {
		cou.indexs = getAllField(p.GetString(couchbaseIndexs, ""))
		if len(cou.indexs) > 0 {
			//fmt.Println("create index ....")
			//fmt.Printf("hasIndex = %v, indexs = %v\n", hasIndex, cou.indexs)
			_ = bu.Manager("", "").CreateIndex(index_name, cou.indexs, true, false)
			cou.hasIndex = hasIndex
		}
	}
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