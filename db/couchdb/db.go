package couchdb

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/zemirco/couchdb"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)


const couchdbIndexs = "couchdb.indexs"


type couchDB struct {
	cli      *couchdb.Client
	database   couchdb.DatabaseService

	indexs []string
	indexId []string
	shouldDropIndex bool
	shouldDropDatabase bool

	keyCount  int64
	fieldCount int64
}
type jsonField struct {
	Fields []string `json:"fields"`
}

type jsonData struct {
	Index jsonField `json:"index"`
	Name string `json:"name"`
}

type DResponse struct {
	Result  string    `json:"result"`
	ID  string  `json:"id"`
	Name string  `json:"name"`
}

type DocumentResponse struct {
	Ok  bool    `json:"ok"`
	Id  string  `json:"id"`
	Rev string  `json:"rev"`
}

func (m *couchDB) Close() error {
	return nil
}

func (m *couchDB) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	return ctx
}

func (m *couchDB) CleanupThread(ctx context.Context) {
	if m.shouldDropIndex && (len(m.indexs) > 0) {
		// 删除所有索引
		start := time.Now()
		for _, indexId := range m.indexId {
			res, err := m.cli.Request(http.MethodDelete, "/db/_index/"+indexId+"/json/test_index", nil, "application/json")
			if err != nil {
				fmt.Printf("[ERROR] drop all indexs error: %v\n", err)
			} else if err == nil && res != nil {
				jsonResult := make(map[string]interface{})
				decoder := json.NewDecoder(res.Body)
				decoder.UseNumber()
				if err = decoder.Decode(&jsonResult); err != nil {
					fmt.Printf("[ERROR] drop all indexs error: %v\n", err)
				} else if ok := jsonResult["ok"].(bool); !ok {
					fmt.Println("[ERROR] failed to drop all indexs")
				}
			}
			defer closeResponseBody(res)
		}
		fmt.Printf("drop all indexs time used: %v\n", time.Now().Sub(start))
	}

	if m.shouldDropDatabase {
		// 删除所有db
		start := time.Now()
		res, err := m.cli.Request(http.MethodDelete, "/db", nil, "application/json")
		if err != nil {
			fmt.Printf("[ERROR] drop all databases error: %v\n", err)
		} else if err == nil && res != nil {
			jsonResult := make(map[string]interface{})
			decoder := json.NewDecoder(res.Body)
			decoder.UseNumber()
			if err = decoder.Decode(&jsonResult); err != nil {
				fmt.Printf("[ERROR] drop all databases error: %v\n", err)
			} else if ok := jsonResult["ok"].(bool); !ok {
				fmt.Println("[ERROR] failed to drop all databases")
			}
		}
		defer closeResponseBody(res)
		fmt.Printf("drop all databases time used: %v\n", time.Now().Sub(start))
	}
}

func (m *couchDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var doc map[string]interface{}
	//d := couchdb.CouchDoc()
	//TODO 应该可以指定要返回哪些fields吧？行为需要与mongodb的实现保持一致
	res, err := m.cli.Request(http.MethodGet, "/db/" + key, nil, "application/json")
	if err != nil {
		fmt.Printf("[ERROR] failed to read couchbase, key = %v, err: %v\n", key, err)
		return nil, err
	}
	defer closeResponseBody(res)
	if res.StatusCode == 200 {
		err = json.NewDecoder(res.Body).Decode(&doc)
		if err != nil {
			fmt.Printf("[ERROR] failed to decode response from 'PUT /{dbname}/{docId}', key = %v, err: %v\n", key, err)
			return nil, err
		}
	} else {
		fmt.Printf("[ERROR] we may can not find document '%v', because the response status code is %v\n", key, res.StatusCode)
	}

	return nil, nil
}

func (m *couchDB) ScanValue(ctx context.Context, table string, count int, values map[string][]byte) ([]map[string][]byte, error) {
	ranKey := m.getRandomKey()
	var doc map[string]interface{}
	res, err := m.cli.Request(http.MethodGet, "/db/" + ranKey, nil, "application/json")
	if err != nil {
		fmt.Printf("[ERROR] failed to read couchbase, key = %v, err: %v\n", ranKey, err)
		return nil, err
	}
	defer closeResponseBody(res)
	if res.StatusCode == 200 {
		err = json.NewDecoder(res.Body).Decode(&doc)
		if err != nil {
			fmt.Printf("[ERROR] failed to decode response from 'PUT /{dbname}/{docId}', key = %v, err: %v\n", ranKey, err)
			return nil, err
		}
	} else {
		fmt.Printf("[ERROR] we may can not find document '%v', because the response status code is %v\n", ranKey, res.StatusCode)
	}
	ranFieldName := m.getRandomField()

	start := time.Now()
	v := doc[ranFieldName]
	var fieldstring = "\"" + ranFieldName + "\"" + ":" + "\"" + v.(string) + "\""
	var selectorStr = "{" + fieldstring +"}"

	var jsonStr string
	if len(m.indexId) > 0 {
		jsonStr = "{\"selector\":" + selectorStr +",\"use_index\":\"test_index_"+ranFieldName+"\"}"
	} else {
		jsonStr = "{\"selector\":" + selectorStr +"}"
	}

	b := bytes.NewBufferString(jsonStr)

	req, err := http.NewRequest(http.MethodPost, "http://127.0.0.1:5984/db/_find", b)
	req.Header.Set("Content-Type", "application/json")
	// req.SetBasicAuth("user", "password")

	client := http.Client{Timeout:time.Second*360000, Jar:m.cli.CookieJar}

	res, err = client.Do(req)
	fmt.Printf("%+v\n", res)
	// res, err = m.cli.Request(http.MethodPost, "/db/_find", b, "application/json;charset=UTF-8")
	if err != nil {
		fmt.Printf("[ERROR] failed to scanvalue couchdb, err: %v\n", err)
		return nil, err
	}
	defer closeResponseBody(res)


	var docs []map[string]interface{}
	var response map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&response)
	if err != nil {
		fmt.Printf("[ERROR] failed to decode response from 'PUT /{dbname}/_find', err: %v\n", err)
		return nil, err
	} else {
		docsVal, ok := response["docs"].([]interface{})
		if !ok {
			fmt.Println("[ERROR] failed to convert 'docs' type from 'PUT /{dbname}/{docId}' response")
		} else if ok && len(docsVal) == 0 {
			fmt.Println(jsonStr)
			fmt.Println("[ERROR] we have not get result from db, the method ScanValue() has exception!!!")
		} else {
			for _, v := range docsVal {
				doc, ok := v.(map[string]interface{})
				if !ok {
					fmt.Println("[ERROR] failed to convert 'docsVal' type from 'PUT /{dbname}/{docId}' response")
				} else {
					docs = append(docs, doc)
				}

			}
		}

	}
	fmt.Printf("==== scan value time used %v\n", time.Now().Sub(start))
	return nil, nil
}


// Insert a document.
func (m *couchDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	var fieldstring string

	i := 0
	for k, v := range values {
		var temp = "\"" + k + "\""
		fieldstring += temp
		fieldstring += ":"
		temp = "\"" + base64.StdEncoding.EncodeToString(v) + "\""
		fieldstring += temp
		i ++
		if i != len(values) {
			fieldstring += ","
		}
	}
	var jsonStr = "{" + fieldstring + "}"
	//var jsonStr = "{" +
	//					"\"docs\":[" +
	//						"{" +
	//	"\"_id\":" + key + "," + fieldstring +
	//						"}" +
	//	"]}"
	// fmt.Println(jsonStr)
	//jsonStr := "{\"docs\":[{\"_id\":\"user1000\",\"field0\":\"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTU\",\"field1\":\"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTU\",\"field2\":\"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTU\"}]}"
	// fmt.Println(jsonStr)
	b := bytes.NewBufferString(jsonStr)

	//res, err :=m.cli.Request(http.MethodPost, "/db/_bulk_docs", b, "application/json;charset=UTF-8")
	res, err :=m.cli.Request(http.MethodPut, "/db/"+key, b, "application/json;charset=UTF-8")
	if err != nil {
		fmt.Printf("[ERROR] failed to insert couchdb, err: %v\n", err)
		return err
	}
	defer closeResponseBody(res)
	if res.StatusCode != 201 && res.StatusCode != 202 {
		fmt.Println("[ERROR] failed to insert a document")
		return errors.New("[ERROR] failed to insert a document")
	}
	var response DocumentResponse

	err = json.NewDecoder(res.Body).Decode(&response)
	if err != nil {
		fmt.Printf("[ERROR] failed to decode response from 'PUT /{dbname}/{docId}', err: %v\n", err)
		return err
	} else if err == nil && !response.Ok {
		fmt.Println("[ERROR] failed to insert a document")
	}
	// fmt.Println(res)
	return nil
}
func (m *couchDB) Delete(ctx context.Context, table string, key string) error {
	return nil
}
func (m *couchDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan is not supported")
}

func (m *couchDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	return nil
}

type couchdbCreator struct {
}

func (c couchdbCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	u, err := url.Parse("http://127.0.0.1:5984/")
	if err != nil {
		return nil, err
	}
	// create a new client
	client, err := couchdb.NewClient(u)
	if err != nil {
		fmt.Printf("[ERROR] failed to create a new client, err: %v\n", err)
		return nil, err
	}
	var db couchdb.DatabaseService
	_, err = client.Request(http.MethodGet, "/db/", nil, "application/json")
	if err != nil {
		if _, err = client.Create("db"); err != nil {
			fmt.Printf("[ERROR] failed to create a new database, err: %v\n", err)
			return nil, err
		}
		db = client.Use("db")
	} else {
		db = client.Use("db")
	}


	//db := client.Use("db")
	//fmt.Println("db:", db)
	//if db == nil {
	//	if _, err = client.Create("db"); err != nil {
	//		panic(err)
	//	}
	//	db = client.Use("db")
	//}
	cou := &couchDB{
		cli: client,
		database: db,
		shouldDropIndex:    p.GetBool(prop.DropIndex, prop.DropIndexDefault),
		shouldDropDatabase: p.GetBool(prop.DropDatabase, prop.DropDatabaseDefault),
		keyCount:           p.GetInt64(prop.RecordCount, prop.RecordCountDefault),
		fieldCount:         p.GetInt64(prop.FieldCount, 5),
	}

	cou.indexs = getAllField(p.GetString(couchdbIndexs, ""))
	if len(cou.indexs) > 0 {
		fmt.Println("create index ....")
		fmt.Printf("indexs = %v\n", cou.indexs)
		start := time.Now()
		for _, k := range cou.indexs {
			var temp []string
			temp = append(temp, k)
			data_object := jsonData{
				Index: jsonField{Fields:temp},
				Name:  "test_index_" + k,
			}

			var b bytes.Buffer
			if err := json.NewEncoder(&b).Encode(data_object); err != nil {
				return nil, err
			}

			res, err := client.Request(http.MethodPost, "/db/_index", &b, "application/json")
			if err != nil {
				return nil, err
			}
			defer closeResponseBody(res)

			if res.StatusCode != 200 {
				fmt.Println("[ERROR] failed to create index by 'POST /db/_index'")
				return nil, errors.New("[ERROR] failed to create index by 'POST /db/_index'")
			}
			var response DResponse
			err = json.NewDecoder(res.Body).Decode(&response)
			cou.indexId = append(cou.indexId, response.ID)


		}
		fmt.Printf("Create index time used: %v\n", time.Now().Sub(start))
	}
	return cou, nil
}

func init() {
	ycsb.RegisterDBCreator("couchdb", couchdbCreator{})
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

func (m *couchDB) getRandomKey() string {
	ran := rand.Int63n(m.keyCount)
	return "user"+strconv.FormatInt(ran, 10)
}

func (m *couchDB) getRandomField() string {
	ran := rand.Int63n(m.fieldCount)
	return "field"+strconv.FormatInt(ran, 10)
}

// closeResponseBody discards the body and then closes it to enable returning it to
// connection pool
func closeResponseBody(resp *http.Response) {
	if resp != nil {
		io.Copy(ioutil.Discard, resp.Body) // discard whatever is remaining of body
		resp.Body.Close()
	}
}
