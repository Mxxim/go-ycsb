package main

import (
	"context"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/network/command"
	"go.mongodb.org/mongo-driver/x/network/connstring"
	"github.com/davegardnerisme/deephash"
	"strconv"
	"time"
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

	Txsuffix = "tx"
	Fromsuffix = "From"
	Tosuffix = "To"
	Blocksuffix = "Block"

	txnum = 10
	blocknum = 10
)

type TransactionRetrievalDoc struct {
	TxHash string	`bson:"txHash" json:"txHash"`
	TxIndex int64	`bson:"txIndex" json:"txIndex"`
	From  string	`bson:"from" json:"from"`
	To    string	`bson:"to" json:"to"`
	Extra string    `bson:"extra" json:"extra"`
}

// @collection: blocks
// @primary key:  blockNumber
// @index: blockNumber + txs.txIndex, blockWriteTime
type BlockRetrievalDoc struct {
	BlockNumber uint64	`bson:"blockNumber" json:"blockNumber"`
	BlockWriteTime int64 `bson:"writeTime" json:"writeTime"`
	Txs []*TransactionRetrievalDoc `bson:"txs" json:"txs"`
}

func getDB() (*mongo.Client, error){
	if _, err := connstring.Parse(mongodbUriDefault); err != nil {
		return nil, errors.New("[connection error] parse URL error")
	}
	ns := command.ParseNamespace(mongodbNamespaceDefault)
	if err := ns.Validate(); err != nil {
		return nil, errors.New("[connection error] ns validate error")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cliOpts := options.Client().ApplyURI(mongodbUriDefault)

	// cliOpts.SetAuth(options.Credential{AuthSource: mongodbAuthdbDefault, Username: mongodbUsername, Password: mongodbPassword})

	cli, err := mongo.Connect(ctx, cliOpts)
	if err != nil {
		return nil, errors.New("[connection error] connect error")
	}
	if err := cli.Ping(ctx, nil); err != nil {
		return nil, errors.New("[connection error] cli ping error")
	}
	// check if auth passed
	if _, err := cli.ListDatabaseNames(ctx, map[string]string{}); err != nil {
		return nil, errors.New("[connection error] auth error")
	}
	fmt.Println("Connected to MongoDB!")
	return cli, nil
}

func makeSomeTx(seed string, num int) []*TransactionRetrievalDoc{
	var txs []*TransactionRetrievalDoc
	index := 0
	for index <= num {
		// Block0Tx0, Block0From0, Block0To0
		indexString := strconv.Itoa(index)
		txTemp := TransactionRetrievalDoc{
			TxHash:  byteString(deephash.Hash(seed + "-" + Txsuffix + indexString)),
			TxIndex: int64(index),
			From:    byteString(deephash.Hash(seed + "-" + Fromsuffix + indexString)),
			To:      byteString(deephash.Hash(seed + "-" + Tosuffix + indexString)),
		}
		s := seed + "-" + Txsuffix + indexString
		fmt.Println(s)
		fmt.Printf("String\t%x\n", deephash.Hash(s))
		fmt.Println(byteString(deephash.Hash(s)))
		fmt.Println("")
		txs = append(txs, &txTemp)
		index++
	}
	return txs
}

func main() {
	cli, err := getDB()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	ns := command.ParseNamespace(mongodbNamespaceDefault)
	coll := cli.Database(ns.DB).Collection(ns.Collection)

	for index := 0; index <= blocknum; index++ {
		// Block0, 10
		txs := makeSomeTx(Blocksuffix + strconv.Itoa(index), txnum)
		B := BlockRetrievalDoc{
			BlockNumber:    uint64(index),
			BlockWriteTime: time.Now().Unix(),
			Txs:            txs,
		}

		_, err := coll.InsertOne(nil, B)
		if err != nil {
			fmt.Println("insert error")
			return
		}

	}

}

func byteString(p []byte) string {
	for i := 0; i < len(p); i++ {
		if p[i] == 0 {
			return string(p[0:i])
		}
	}
	return string(p)
}

