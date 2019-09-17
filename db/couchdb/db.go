package couchdb

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/zemirco/couchdb"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)


const couchdbIndexs = "couchdb.indexs"


type couchDB struct {
	cli      *couchdb.Client
	database   couchdb.DatabaseService

	hasIndex bool
	indexs []string
	shouldDropIndex bool
}
type jsonField struct {
	Fields []string `json:"fields"`
}

type jsonData struct {
	Index jsonField `json:"index"`
	Name string `json:"name"`
}

type dummyDocument struct {
	couchdb.Document
	Data string
}

func (m *couchDB) Close() error {
	return nil
}

func (m *couchDB) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	return ctx
}

func (m *couchDB) CleanupThread(ctx context.Context) {
}

func (m *couchDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var doc map[string][]byte
	//d := couchdb.CouchDoc()
	res, err := m.cli.Request(http.MethodGet, "/db/" + key, nil, "application/json")
	if err != nil {
		 panic(err)
	}
	defer res.Body.Close()
	_ = json.NewDecoder(res.Body).Decode(doc)
	return doc, nil
}

func (m *couchDB) ScanValue(ctx context.Context, table string, count int, values map[string][]byte) ([]map[string][]byte, error) {
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
	var selectorStr = "{" + fieldstring +"}"
	var jsonStr = "{\"selector\":" + selectorStr +",\"use_index\":\"test_index\"}"

	b := bytes.NewBufferString(jsonStr)
	_, err := m.cli.Request(http.MethodPost, "/db/_find", b, "application/json;charset=UTF-8")

	if err != nil {
		panic(err)
	}
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
	key = "\"" + key + "\""
	var jsonStr = "{" +
						"\"docs\":[" +
							"{" +
		"\"_id\":" + key + "," + fieldstring +
							"}" +
		"]}"
	// fmt.Println(jsonStr)
	//jsonStr := "{\"docs\":[{\"_id\":\"user1000\",\"field0\":\"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTU\",\"field1\":\"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTU\",\"field2\":\"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTU\"}]}"

	b := bytes.NewBufferString(jsonStr)


	_, err :=m.cli.Request(http.MethodPost, "/db/_bulk_docs", b, "application/json;charset=UTF-8")
	if err != nil {
		panic(err)
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
		panic(err)
	}
	// create a new client
	client, err := couchdb.NewClient(u)
	if err != nil {
		panic(err)
	}
	var db couchdb.DatabaseService
	_, err = client.Request(http.MethodGet, "/db/", nil, "application/json")
	if err != nil {
		if _, err = client.Create("db"); err != nil {
			panic(err)
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

	}
	hasIndex := p.GetBool(prop.HasIndex, prop.HasIndexDefault)
	if hasIndex {
		cou.indexs = getAllField(p.GetString(couchdbIndexs, ""))
		if len(cou.indexs) > 0 {
			//fmt.Println("create index ....")
			//fmt.Printf("hasIndex = %v, indexs = %v\n", hasIndex, cou.indexs)
			//err = bu.Manager("", "").CreateIndex(index_name, cou.indexs, true, false)

			data_field := jsonField{Fields:cou.indexs}
			data_object := jsonData{
				Index: data_field,
				Name:  "test_index",
			}

			var b bytes.Buffer
			if err := json.NewEncoder(&b).Encode(data_object); err != nil {
				return nil, err
			}

			res, err := client.Request(http.MethodPost, "/db/_index", &b, "application/json")
			if err != nil {
				panic(err)
			}
			defer res.Body.Close()
			var response couchdb.DocumentResponse
			_ = json.NewDecoder(res.Body).Decode(&response)


			cou.hasIndex = hasIndex
		}
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

func Post(url string, data interface{}, contentType string) (content string) {
	jsonStr, _ := json.Marshal(data)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	req.Header.Add("content-type", contentType)
	if err != nil {
		panic(err)
	}
	defer req.Body.Close()

	client := &http.Client{Timeout: 5 * time.Second}
	resp, error := client.Do(req)
	if error != nil {
		panic(error)
	}
	defer resp.Body.Close()

	result, _ := ioutil.ReadAll(resp.Body)
	content = string(result)
	return
}
