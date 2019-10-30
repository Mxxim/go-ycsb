package main

import (
	"context"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/network/command"
	"go.mongodb.org/mongo-driver/x/network/connstring"
	"math/rand"
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

	txnum = 250
	blocknum = 3000


	TxHashlength = 64
	AddressHashlength = 40

	SolutionOneId = "S1-ID"
	SolutionOneNoId = "S1-NoID"

	SolutionTwoId = "S2-ID"
	SolutionTwoNoId = "S2-NoID"

	SolutionThreeTx = "S3-Tx"
	SolutionThreeBlock = "S3-Block"

)
//

// S1 index
// col = db.getCollection("S1-NoID");col.createIndex( { "txs.txHash": 1} );col.createIndex( { "writeTime": 1} );

// S2 index
// col = db.getCollection("S2-NoID");col.createIndex( { "txHash": 1} );col.createIndex( { "block.writeTime": 1} );

// S3 index
// col = db.getCollection("S3-Tx");col.createIndex( { "txHash": 1} )
// col = db.getCollection("S3-Block");col.createIndex( { "writeTime": 1} )

// 方案一：采用嵌套的方式，区块里嵌套交易
// @collection: blocks
// @primary key:  blockNumber
// @index: txs.txhash, BlockWriteTime；
type TransactionRetrievalDoc struct {
	TxHash string	`bson:"txHash" json:"txHash"`
	TxIndex int64	`bson:"txIndex" json:"txIndex"`
	From  string	`bson:"from" json:"from"`
	To    string	`bson:"to" json:"to"`
	Extra string    `bson:"extra" json:"extra"`
}
type BlockRetrievalDoc struct {
	BlockNumber uint64	`bson:"blockNumber" json:"blockNumber"`
	BlockWriteTime int64 `bson:"writeTime" json:"writeTime"`
	Txs []*TransactionRetrievalDoc `bson:"txs" json:"txs"`
}

type BlockRetrievalDoc12 struct {
	BlockNumber uint64	`bson:"_id" json:"_id"`
	BlockWriteTime int64 `bson:"writeTime" json:"writeTime"`
	Txs []*TransactionRetrievalDoc `bson:"txs" json:"txs"`
}

// 方案二：采用嵌套的方式，交易里嵌套区块
// @index: Txhash, block.BlockWriteTime；
type TransactionRetrievalDoc2 struct {
	TxHash string	`bson:"txHash" json:"txHash"`
	TxIndex int64	`bson:"txIndex" json:"txIndex"`
	From  string	`bson:"from" json:"from"`
	To    string	`bson:"to" json:"to"`
	Extra string    `bson:"extra" json:"extra"`
	Block BlockRetrievalDoc2 `bson:"block" json:"block"`
}
type TransactionRetrievalDoc22 struct {
	TxHash string	`bson:"_id" json:"_id"`
	TxIndex int64	`bson:"txIndex" json:"txIndex"`
	From  string	`bson:"from" json:"from"`
	To    string	`bson:"to" json:"to"`
	Extra string    `bson:"extra" json:"extra"`
	Block BlockRetrievalDoc2 `bson:"block" json:"block"`
}

type BlockRetrievalDoc2 struct {
	BlockNumber uint64	`bson:"blockNumber" json:"blockNumber"`
	BlockWriteTime int64 `bson:"writeTime" json:"writeTime"`
}

// 方案三：采用引用的方式，存在交易集合与区块集合
// index : txHash
type TransactionRetrievalDoc3 struct {
	TxHash string	`bson:"txHash" json:"txHash"`
	TxIndex int64	`bson:"txIndex" json:"txIndex"`
	From  string	`bson:"from" json:"from"`
	To    string	`bson:"to" json:"to"`
	Extra string    `bson:"extra" json:"extra"`
	BlockNumber uint64 `bson:"blockNumber" json:"blockNumber"`
}

// index: BlockWriteTime
type BlockRetrievalDoc3 struct {
	BlockNumber uint64	`bson:"blockNumber" json:"blockNumber"`
	BlockWriteTime int64 `bson:"writeTime" json:"writeTime"`
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

func generateTx(seed int) ([]byte, []byte, []byte){
	TxHashByte := make([]byte, TxHashlength)
	FromHashByte := make([]byte, AddressHashlength)
	ToHashByte := make([]byte, AddressHashlength)
	RandBytes(rand.New(rand.NewSource(time.Now().UnixNano()+ int64(seed))), TxHashByte)
	RandBytes(rand.New(rand.NewSource(time.Now().UnixNano())), FromHashByte)
	RandBytes(rand.New(rand.NewSource(time.Now().UnixNano())), ToHashByte)
	return TxHashByte, FromHashByte, ToHashByte
}

func makeSomeTx(seed string, num int) []*TransactionRetrievalDoc{
	var txs []*TransactionRetrievalDoc
	index := 1
	for index <= num {
		TxHashByte, FromHashByte, ToHashByte := generateTx(index)
		txTemp := TransactionRetrievalDoc{
			TxHash:  "0x" + string(TxHashByte),
			TxIndex: int64(index),
			From:    "0x" + string(FromHashByte),
			To:      "0x" + string(ToHashByte),
			Extra: "hello, world",
		}
		txs = append(txs, &txTemp)
		index++
	}
	return txs
}

func SolutionOne(coll *mongo.Collection) error{

	for index := 1; index <= blocknum; index++ {
		// Block0, 10
		txs := makeSomeTx(Blocksuffix + strconv.Itoa(index), txnum)
		var B interface{}
		if coll.Name() == SolutionOneNoId {
			B = BlockRetrievalDoc{
				BlockNumber:    uint64(index),
				BlockWriteTime: time.Now().UnixNano(),
				Txs:            txs,
			}
		} else if coll.Name() == SolutionOneId {
			B = BlockRetrievalDoc12{
				BlockNumber:    uint64(index),
				BlockWriteTime: time.Now().UnixNano(),
				Txs:            txs,
			}
		}
		_, err := coll.InsertOne(nil, B)
		if err != nil {
			fmt.Println("[S1] insert error")
			return err
		}

	}
	return nil
}
//  col = db.getCollection("S2-ID");col.find({"_id": ""}).pretty()
func SolutionTwo(coll *mongo.Collection) error{
	var B interface{}
	for bindex := 1; bindex <= blocknum; bindex++ {
		B = BlockRetrievalDoc2{
			BlockNumber:    uint64(bindex),
			BlockWriteTime: time.Now().UnixNano(),
		}
		// Block0, 10
		for tindex := 1; tindex <= txnum; tindex++ {
			var T interface{}
			TxHashByte, FromHashByte, ToHashByte := generateTx(tindex)
			if coll.Name() == SolutionTwoId {
				T = TransactionRetrievalDoc22{
					TxHash:  "0x" + string(TxHashByte),
					TxIndex: int64(tindex),
					From:    "0x" + string(FromHashByte),
					To:      "0x" + string(ToHashByte),
					Extra: "hello, world",
					Block:   B.(BlockRetrievalDoc2),
				}
			} else if coll.Name() == SolutionTwoNoId {
				T = TransactionRetrievalDoc2{
					TxHash:  "0x" + string(TxHashByte),
					TxIndex: int64(tindex),
					From:    "0x" + string(FromHashByte),
					To:      "0x" + string(ToHashByte),
					Extra: "hello, world",
					Block:   B.(BlockRetrievalDoc2),
				}
			}
			_, err := coll.InsertOne(nil, T)
			if err != nil {
				fmt.Println("[S2] insert error, continue")
				fmt.Printf(err.Error())
				fmt.Printf("%+v\n", T)
			}
		}
	}
	return nil
}

func SolutionThree(Txcoll *mongo.Collection, Blockcoll *mongo.Collection) error{
	for bindex := 1; bindex <= blocknum; bindex++ {
		B := BlockRetrievalDoc3{
			BlockNumber:    uint64(bindex),
			BlockWriteTime: time.Now().UnixNano(),
		}
		_, err := Blockcoll.InsertOne(nil, B)
		if err != nil {
			fmt.Println("[S3] insert block error")
			return err
		}

		for tindex := 1; tindex <= txnum; tindex++ {
			var T interface{}
			TxHashByte, FromHashByte, ToHashByte := generateTx(tindex)
			T = TransactionRetrievalDoc3{
				TxHash:  "0x" + string(TxHashByte),
				TxIndex: int64(tindex),
				From:    "0x" + string(FromHashByte),
				To:      "0x" + string(ToHashByte),
				Extra: "hello, world",
				BlockNumber: uint64(bindex),
			}
			_, err := Txcoll.InsertOne(nil, T)
			if err != nil {
				fmt.Println("[S3] insert tx error")
				return err
			}
		}
	}
	return nil
}
func main() {
	cli, err := getDB()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	ns := command.ParseNamespace(mongodbNamespaceDefault)

	//coll := cli.Database(ns.DB).Collection(SolutionOneNoId)
	//err = SolutionOne(coll)
	//if err != nil {
	//	fmt.Println(err.Error())
	//	return
	//}
	//
	//coll = cli.Database(ns.DB).Collection(SolutionOneId)
	//err = SolutionOne(coll)
	//if err != nil {
	//	fmt.Println(err.Error())
	//	return
	//}
	//
	//coll = cli.Database(ns.DB).Collection(SolutionTwoNoId)
	//err = SolutionTwo(coll)
	//if err != nil {
	//	fmt.Println(err.Error())
	//	return
	//}

	coll := cli.Database(ns.DB).Collection(SolutionTwoId)
	err = SolutionTwo(coll)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	Txcoll := cli.Database(ns.DB).Collection(SolutionThreeTx)
	Blockcoll := cli.Database(ns.DB).Collection(SolutionThreeBlock)
	err = SolutionThree(Txcoll, Blockcoll)
	if err != nil {
		fmt.Println(err.Error())
		return
	}



}

var letters = []byte("1234567890abcdef")

// RandBytes fills the bytes with alphabetic characters randomly
func RandBytes(r *rand.Rand, b []byte) {
	for i := range b {
		b[i] = letters[r.Intn(len(letters))]
	}
}

// col = db.getCollection("S1-ID");col.find().pretty()
