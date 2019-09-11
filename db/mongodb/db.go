package mongodb

import (
	"context"
	"errors"
	"fmt"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"go.mongodb.org/mongo-driver/x/network/command"
	"go.mongodb.org/mongo-driver/x/network/connstring"
	"strings"
)

const (
	mongodbUri       = "mongodb.uri"
	mongodbNamespace = "mongodb.namespace"
	mongodbAuthdb    = "mongodb.authdb"
	mongodbUsername  = "mongodb.username"
	mongodbPassword  = "mongodb.password"
	mongodbIndexs	 = "mongodb.indexs"

	mongodbUriDefault       = "mongodb://127.0.0.1:27017"
	mongodbNamespaceDefault = "ycsb.ycsb"
	mongodbAuthdbDefault    = "admin"
)

type mongoDB struct {
	cli      *mongo.Client
	dbname   string
	collname string
	coll     *mongo.Collection

	hasIndex bool
	indexs []string
	shouldDropIndex bool
}

func (m *mongoDB) Close() error {
	return m.cli.Disconnect(context.Background())
}

func (m *mongoDB) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	return ctx
}

func (m *mongoDB) CleanupThread(ctx context.Context) {
	if m.shouldDropIndex {
		// 删除所有索引
		indexView := m.coll.Indexes()
		_, err := indexView.DropAll(context.Background(), options.DropIndexes())
		if err != nil {
			fmt.Printf("Drop error: %s\n", err.Error())
			return
		}
	}
}

// Read a document.
func (m *mongoDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	projection := map[string]bool{"_id": false}
	for _, field := range fields {
		projection[field] = true
	}
	opt := &options.FindOneOptions{Projection: projection}
	var doc map[string][]byte
	if err := m.coll.FindOne(ctx, bson.M{"_id": key}, opt).Decode(&doc); err != nil {
		return nil, fmt.Errorf("Read error: %s", err.Error())
	}
	return doc, nil
}

// Scan documents.
func (m *mongoDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	projection := map[string]bool{"_id": false}
	for _, field := range fields {
		projection[field] = true
	}
	limit := int64(count)
	opt := &options.FindOptions{Projection: projection, Sort: bson.M{"_id": 1}, Limit: &limit}
	cursor, err := m.coll.Find(ctx, bson.M{"_id": bson.M{"$gte": startKey}}, opt)
	if err != nil {
		return nil, fmt.Errorf("Scan error: %s", err.Error())
	}
	defer cursor.Close(ctx)
	var docs []map[string][]byte
	for cursor.Next(ctx) {
		var doc map[string][]byte
		if err := cursor.Decode(&doc); err != nil {
			return docs, fmt.Errorf("Scan error: %s", err.Error())
		}
		docs = append(docs, doc)
	}
	return docs, nil
}

// Insert a document.
func (m *mongoDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	//fmt.Printf("======= mongodb insert, key = %v, len(values) = %v\n", key, len(values))
	doc := bson.M{"_id": key}
	for k, v := range values {
		//fmt.Println("------->>>>>>>")
		//fmt.Println(k, v)
		doc[k] = v
	}
	if _, err := m.coll.InsertOne(ctx, doc); err != nil {
		fmt.Println(err)
		return fmt.Errorf("Insert error: %s", err.Error())
	}
	return nil
}

// Update a document.
func (m *mongoDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	res, err := m.coll.UpdateOne(ctx, bson.M{"_id": key}, bson.M{"$set": values})
	if err != nil {
		return fmt.Errorf("Update error: %s", err.Error())
	}
	if res.MatchedCount != 1 {
		return fmt.Errorf("Update error: %s not found", key)
	}
	return nil
}

// Delete a document.
func (m *mongoDB) Delete(ctx context.Context, table string, key string) error {
	res, err := m.coll.DeleteOne(ctx, bson.M{"_id": key})
	if err != nil {
		return fmt.Errorf("Delete error: %s", err.Error())
	}
	if res.DeletedCount != 1 {
		return fmt.Errorf("Delete error: %s not found", key)
	}
	return nil
}

func (m *mongoDB) ScanValue(ctx context.Context, table string, count int, values map[string][]byte) ([]map[string][]byte, error) {
	//fmt.Printf("=== mongodb scanvalue, count = %v, len(values) = %v\n", count, len(values))
	projection := map[string]bool{"_id": false}
	limit := int64(count)
	opt := &options.FindOptions{Projection: projection, Sort: bson.M{"_id": 1}, Limit: &limit}

	bsonm := make(bson.M)
	for k, v := range values {
		//fmt.Println(k, v)
		bsonm[k] = v
	}

	cursor, err := m.coll.Find(ctx, bsonm, opt)
	if err != nil {
		return nil, fmt.Errorf("Scan error: %s", err.Error())
	}
	defer cursor.Close(ctx)
	var docs []map[string][]byte
	for cursor.Next(ctx) {
		var doc map[string][]byte
		if err := cursor.Decode(&doc); err != nil {
			return docs, fmt.Errorf("Scan error: %s", err.Error())
		}
		//fmt.Println(doc)
		docs = append(docs, doc)
	}
	return docs, nil
}

type mongodbCreator struct {
}

func (c mongodbCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	uri := p.GetString(mongodbUri, mongodbUriDefault)
	nss := p.GetString(mongodbNamespace, mongodbNamespaceDefault)
	authdb := p.GetString(mongodbAuthdb, mongodbAuthdbDefault)

	if _, err := connstring.Parse(uri); err != nil {
		return nil, err
	}
	ns := command.ParseNamespace(nss)
	if err := ns.Validate(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cliOpts := options.Client().ApplyURI(uri)

	username, usrExist := p.Get(mongodbUsername)
	password, pwdExist := p.Get(mongodbPassword)
	if usrExist && pwdExist {
		cliOpts.SetAuth(options.Credential{AuthSource: authdb, Username: username, Password: password})
	} else if usrExist {
		return nil, errors.New("mongodb.username is set, but mongodb.password is missing")
	} else if pwdExist {
		return nil, errors.New("mongodb.password is set, but mongodb.username is missing")
	}

	cli, err := mongo.Connect(ctx, cliOpts)
	if err != nil {
		return nil, err
	}
	if err := cli.Ping(ctx, nil); err != nil {
		return nil, err
	}
	// check if auth passed
	if _, err := cli.ListDatabaseNames(ctx, map[string]string{}); err != nil {
		return nil, errors.New("auth failed")
	}

	fmt.Println("Connected to MongoDB!")

	coll := cli.Database(ns.DB).Collection(ns.Collection)
	m := &mongoDB{
		cli:      cli,
		dbname:   ns.DB,
		collname: ns.Collection,
		coll:     coll,
		shouldDropIndex: p.GetBool(prop.DropIndex, prop.DropIndexDefault),
	}

	hasIndex := p.GetBool(prop.HasIndex, prop.HasIndexDefault)
	if hasIndex {
		m.indexs = getAllField(p.GetString(mongodbIndexs, ""))
		if len(m.indexs) > 0 {
			fmt.Println("create index ....")
			fmt.Printf("hasIndex = %v, indexs = %v\n", hasIndex, m.indexs)
			indexView := coll.Indexes()

			var bsonxD bsonx.Doc
			for _, fieldKey := range m.indexs {
				bsonxD = append(bsonxD, bsonx.Elem{fieldKey, bsonx.Int32(1)})
			}

			_, err = indexView.CreateOne(context.Background(), mongo.IndexModel{
				Keys: bsonxD,
				Options: options.Index().SetName("fieldIndex"),
			})
			if err != nil {
				return nil, err
			}
			m.hasIndex = hasIndex
		}
	}
	return m, nil
}

func init() {
	ycsb.RegisterDBCreator("mongodb", mongodbCreator{})
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