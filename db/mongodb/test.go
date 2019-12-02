package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
	//"go.mongodb.org/mongo-driver/x/network/command"
	//"go.mongodb.org/mongo-driver/x/network/connstring"
	"math/big"
	"sync"
	"time"
)

const (
	mongodbUri       = "mongodb.uri"
	mongodbNamespace = "mongodb.namespace"
	mongodbAuthdb    = "mongodb.authdb"
	mongodbUsername  = "mongodb.username"
	mongodbPassword  = "mongodb.password"
	mongodbIndexs    = "mongodb.indexs"

	mongodbUriDefault       = "mongodb://127.0.0.1:27017"
	mongodbNamespaceDefault = "flato.flato"
	mongodbAuthdbDefault    = "admin"

	start       = 1
	txnum       = 500
	blocknum    = 50000
	createIndex = true

	TxHashlength      = 64
	AddressHashlength = 40

	SolutionOneBlocks = "S1-blocks"

	SolutionTwoBlocks       = "S2-blocks"
	SolutionTwoTransactions = "S2-transactions"

	SolutionThreeBlocks       = "S3-blocks"
	SolutionThreeTransactions = "S3-transactions"

	SolutionFourBlocks       = "S4-blocks"
	SolutionFourTransactions = "S4-transactions"

	SolutionFiveBlocks       = "S5-blocks"
	SolutionFiveTransactions = "S5-transactions"
)

// 方案一：采用嵌套的方式，区块里嵌套交易
// @collection: blocks
// @primary key:  blockNumber
// @index: writeTime, txs.hash, txs.from, txs.to；
type BlockRetrievalDoc struct {
	BlockNumber    uint64                     `bson:"_id" json:"_id"`
	BlockWriteTime int64                      `bson:"writeTime" json:"writeTime"`
	Txs            []*TransactionRetrievalDoc `bson:"txs,omitempty" json:"txs,omitempty"`
}

type TransactionRetrievalDoc struct {
	TxHash  string `bson:"hash" json:"hash"`
	TxIndex int64  `bson:"index" json:"index"`
	From    string `bson:"from" json:"from"`
	To      string `bson:"to" json:"to"`
	Extra   string `bson:"extra,omitempty" json:"extra,omitempty"`
}

// **方案二：采用引用的方式，交易集合和区块集合，_id不为交易哈希
// @collection: transactions
// @primary key: _id
// @index: hash, (blkNum), from, to
// 已知blockNumber可以直接去filelog得到所有信息了，不需要在索引数据库里查询，因此没必要创建blockNumber二层索引
type TransactionRetrievalDoc2 struct {
	TxHash      string `bson:"hash" json:"hash"`
	TxIndex     int64  `bson:"index" json:"index"`
	From        string `bson:"from" json:"from"`
	To          string `bson:"to" json:"to"`
	Extra       string `bson:"extra,omitempty" json:"extra,omitempty"`
	BlockNumber uint64 `bson:"blkNum" json:"blkNum"`
}

// @collection: blocks
// @primary key: blockNumber
// @index: writeTime
type BlockRetrievalDoc23 struct {
	BlockNumber    uint64 `bson:"_id" json:"blockNumber"`
	BlockWriteTime int64  `bson:"writeTime" json:"writeTime"`
}

// 方案三：采用引用的方式，交易集合和区块集合，_id为交易哈希
// @collection: transactions
// @primary key: txHash
// @index: (blkNum), from, to
type TransactionRetrievalDoc3 struct {
	TxHash      string `bson:"_id" json:"txHash"`
	TxIndex     int64  `bson:"index" json:"index"`
	From        string `bson:"from" json:"from"`
	To          string `bson:"to" json:"to"`
	Extra       string `bson:"extra,omitempty" json:"extra,omitempty"`
	BlockNumber uint64 `bson:"blkNum" json:"blkNum"`
}

// 方案四：采用引用的方式，交易集合和区块集合，_id不为交易哈希
// @collection: transactions
// @primary key: _id
// @index: hash, from, to
type TransactionRetrievalDoc4 struct {
	TxHash      string `bson:"hash" json:"hash"`
	TxIndex     int64  `bson:"index" json:index"`
	From        string `bson:"from" json:"from"`
	To          string `bson:"to" json:"to"`
	Extra       string `bson:"extra,omitempty" json:"extra,omitempty"`
	BlockNumber uint64 `bson:"blkNum" json:"blkNum"`
}

// @collection: blocks
// @primary key: blockNumber
// @index: writeTime
type BlockRetrievalDoc45 struct {
	BlockNumber    uint64   `bson:"_id" json:"blockNumber"`
	BlockWriteTime int64    `bson:"writeTime" json:"writeTime"`
	Txs            []string `bson:"txs,omitempty" json:"txs,omitempty"`
}

// 方案五：采用引用的方式，交易集合和区块集合，_id为交易哈希
// @collection: transactions
// @primary key: txHash
// @index: from, to
type TransactionRetrievalDoc5 struct {
	TxHash      string `bson:"_id" json:"txHash"`
	TxIndex     int64  `bson:"index" json:"index"`
	From        string `bson:"from" json:"from"`
	To          string `bson:"to" json:"to"`
	Extra       string `bson:"extra,omitempty" json:"extra,omitempty"`
	BlockNumber uint64 `bson:"blkNum" json:"blkNum"`
}

func getDB() (*mongo.Client, error) {
	//if _, err := connstring.Parse(mongodbUriDefault); err != nil {
	//	return nil, errors.New("[connection error] parse URL error")
	//}
	//ns := command.ParseNamespace(mongodbNamespaceDefault)
	//if err := ns.Validate(); err != nil {
	//	return nil, errors.New("[connection error] ns validate error")
	//}

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

var hashBuffer = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, TxHashlength)
		return &buf
	},
}

var addressBuffer = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, AddressHashlength)
		return &buf
	},
}

func generateTx() ([]byte, []byte, []byte) {

	TxHashBytes := hashBuffer.Get().(*[]byte)
	defer hashBuffer.Put(TxHashBytes)

	FromHashBytes := addressBuffer.Get().(*[]byte)
	defer addressBuffer.Put(FromHashBytes)

	ToHashBytes := addressBuffer.Get().(*[]byte)
	defer addressBuffer.Put(ToHashBytes)

	TxHashByte := *TxHashBytes
	FromHashByte := *FromHashBytes
	ToHashByte := *ToHashBytes

	RandBytes(TxHashByte)
	RandBytes(FromHashByte)
	RandBytes(ToHashByte)
	return TxHashByte, FromHashByte, ToHashByte
}

func makeSomeTx() []*TransactionRetrievalDoc {
	var txs []*TransactionRetrievalDoc
	index := 1
	for index <= txnum {
		TxHashByte, FromHashByte, ToHashByte := generateTx()
		txTemp := TransactionRetrievalDoc{
			TxHash:  "0x" + string(TxHashByte),
			TxIndex: int64(index),
			From:    "0x" + string(FromHashByte),
			To:      "0x" + string(ToHashByte),
			Extra:   "hello, world",
		}
		txs = append(txs, &txTemp)
		index++
	}
	return txs
}

//  col = db.getCollection("S1-blocks");col.find({"_id": ""}).pretty()
func SolutionOne(coll *mongo.Collection) error {
	fmt.Printf("---- start insert SolutionOne data...createIndex = %v\n----", createIndex)
	now := time.Now()

	if createIndex {
		fmt.Printf("create index ...., now time is %v\n", time.Now())
		start := time.Now()

		// 创建非组合索引
		var indexModels []mongo.IndexModel
		indexModels = append(indexModels, mongo.IndexModel{
			Keys: bson.D{
				{"writeTime", 1},
			},
		})

		indexModels = append(indexModels, mongo.IndexModel{
			Keys: bson.D{
				{"txs.hash", 1},
			},
		})

		indexModels = append(indexModels, mongo.IndexModel{
			Keys: bson.D{
				{"txs.from", 1},
			},
		})

		indexModels = append(indexModels, mongo.IndexModel{
			Keys: bson.D{
				{"txs.to", 1},
			},
		})

		indexView := coll.Indexes()
		_, err := indexView.CreateMany(context.Background(), indexModels)
		if err != nil {
			return err
		}

		fmt.Printf("Create index time used: %v\n", time.Now().Sub(start))
	}

	for index := start; index <= blocknum; index++ {
		// Block0, 10
		txs := makeSomeTx()
		B := BlockRetrievalDoc{
			BlockNumber:    uint64(index),
			BlockWriteTime: time.Now().UnixNano(),
			Txs:            txs,
		}

		_, err := coll.InsertOne(nil, B)
		if err != nil {
			fmt.Printf("[S1] insert error, now number is %v, err: %v\n", index, err.Error())
			continue
		}

	}
	fmt.Printf("finish SolutionOne, time used: %v\n", time.Now().Sub(now))
	return nil
}

func SolutionTwo(Txcoll *mongo.Collection, Blockcoll *mongo.Collection) error {
	fmt.Printf("---- start insert SolutionTwo data... createIndex = %v\n----", createIndex)
	now := time.Now()
	if createIndex {
		fmt.Printf("create txColl index ...., now time is %v\n", time.Now())
		start := time.Now()

		// 创建非组合索引
		var indexModels []mongo.IndexModel
		var bsonxD bsonx.Doc
		bsonxD = []bsonx.Elem{bsonx.Elem{"hash", bsonx.Int32(1)}}
		indexModels = append(indexModels, mongo.IndexModel{
			Keys: bsonxD,
		})

		//bsonxD = []bsonx.Elem{bsonx.Elem{"blkNum", bsonx.Int32(1)}}
		//indexModels = append(indexModels, mongo.IndexModel{
		//	Keys: bsonxD,
		//})

		bsonxD = []bsonx.Elem{bsonx.Elem{"from", bsonx.Int32(1)}}
		indexModels = append(indexModels, mongo.IndexModel{
			Keys: bsonxD,
		})

		bsonxD = []bsonx.Elem{bsonx.Elem{"to", bsonx.Int32(1)}}
		indexModels = append(indexModels, mongo.IndexModel{
			Keys: bsonxD,
		})

		indexView := Txcoll.Indexes()
		_, err := indexView.CreateMany(context.Background(), indexModels)
		if err != nil {
			return err
		}

		fmt.Printf("Create txColl index time used: %v\n", time.Now().Sub(start))

		fmt.Printf("create blockColl index ...., now time is %v\n", time.Now())
		start = time.Now()

		var blockIndexModels []mongo.IndexModel
		bsonxD = []bsonx.Elem{bsonx.Elem{"writeTime", bsonx.Int32(1)}}
		blockIndexModels = append(blockIndexModels, mongo.IndexModel{
			Keys: bsonxD,
		})

		indexView = Blockcoll.Indexes()
		_, err = indexView.CreateMany(context.Background(), blockIndexModels)
		if err != nil {
			return err
		}

		fmt.Printf("Createb lockColl index time used: %v\n", time.Now().Sub(start))
	}

	for bindex := start; bindex <= blocknum; bindex++ {
		B := BlockRetrievalDoc23{
			BlockNumber:    uint64(bindex),
			BlockWriteTime: time.Now().UnixNano(),
		}
		_, err := Blockcoll.InsertOne(nil, B)
		if err != nil {
			fmt.Printf("[S3] insert block error, now number is %v, err: %v\n", bindex, err.Error())
			continue
		}

		for tindex := 1; tindex <= txnum; tindex++ {
			var T interface{}
			TxHashByte, FromHashByte, ToHashByte := generateTx()
			T = TransactionRetrievalDoc2{
				TxHash:      "0x" + string(TxHashByte),
				TxIndex:     int64(tindex),
				From:        "0x" + string(FromHashByte),
				To:          "0x" + string(ToHashByte),
				Extra:       "hello, world",
				BlockNumber: uint64(bindex),
			}
			_, err := Txcoll.InsertOne(nil, T)
			if err != nil {
				fmt.Printf("[S3] insert tx error, now number is %v, err: %v\n", bindex, err.Error())
				continue
			}
		}
	}
	fmt.Printf("finish SolutionTwo, time used: %v\n", time.Now().Sub(now))
	return nil
}

func SolutionThree(Txcoll *mongo.Collection, Blockcoll *mongo.Collection) error {
	fmt.Printf("---- start insert SolutionThree data... createIndex = %v\n----", createIndex)
	now := time.Now()
	if createIndex {
		fmt.Printf("create txColl index ...., now time is %v\n", time.Now())
		start := time.Now()

		// 创建非组合索引
		var indexModels []mongo.IndexModel
		var bsonxD bsonx.Doc
		//bsonxD = []bsonx.Elem{bsonx.Elem{"blkNum", bsonx.Int32(1)}}
		//indexModels = append(indexModels, mongo.IndexModel{
		//	Keys: bsonxD,
		//})

		bsonxD = []bsonx.Elem{bsonx.Elem{"from", bsonx.Int32(1)}}
		indexModels = append(indexModels, mongo.IndexModel{
			Keys: bsonxD,
		})

		bsonxD = []bsonx.Elem{bsonx.Elem{"to", bsonx.Int32(1)}}
		indexModels = append(indexModels, mongo.IndexModel{
			Keys: bsonxD,
		})

		indexView := Txcoll.Indexes()
		_, err := indexView.CreateMany(context.Background(), indexModels)
		if err != nil {
			return err
		}

		fmt.Printf("Create txColl index time used: %v\n", time.Now().Sub(start))

		fmt.Printf("create blockColl index ...., now time is %v\n", time.Now())
		start = time.Now()

		var blockIndexModels []mongo.IndexModel
		bsonxD = []bsonx.Elem{bsonx.Elem{"writeTime", bsonx.Int32(1)}}
		blockIndexModels = append(blockIndexModels, mongo.IndexModel{
			Keys: bsonxD,
		})

		indexView = Blockcoll.Indexes()
		_, err = indexView.CreateMany(context.Background(), blockIndexModels)
		if err != nil {
			return err
		}

		fmt.Printf("Createb lockColl index time used: %v\n", time.Now().Sub(start))
	}

	for bindex := start; bindex <= blocknum; bindex++ {
		B := BlockRetrievalDoc23{
			BlockNumber:    uint64(bindex),
			BlockWriteTime: time.Now().UnixNano(),
		}
		_, err := Blockcoll.InsertOne(nil, B)
		if err != nil {
			fmt.Printf("[S3] insert block error, now number is %v, err: %v\n", bindex, err.Error())
			continue
		}

		for tindex := 1; tindex <= txnum; tindex++ {
			var T interface{}
			TxHashByte, FromHashByte, ToHashByte := generateTx()
			T = TransactionRetrievalDoc3{
				TxHash:      "0x" + string(TxHashByte),
				TxIndex:     int64(tindex),
				From:        "0x" + string(FromHashByte),
				To:          "0x" + string(ToHashByte),
				Extra:       "hello, world",
				BlockNumber: uint64(bindex),
			}
			_, err := Txcoll.InsertOne(nil, T)
			if err != nil {
				fmt.Printf("[S3] insert tx error, now number is %v, err: %v\n", bindex, err.Error())
				continue
			}
		}
	}
	fmt.Printf("finish SolutionThree, time used: %v\n", time.Now().Sub(now))
	return nil
}

func SolutionFour(Txcoll *mongo.Collection, Blockcoll *mongo.Collection) error {
	fmt.Printf("---- start insert SolutionFour data... createIndex = %v\n----", createIndex)
	now := time.Now()
	if createIndex {
		fmt.Printf("create txColl index ...., now time is %v\n", time.Now())
		start := time.Now()

		// 创建非组合索引
		var indexModels []mongo.IndexModel
		var bsonxD bsonx.Doc
		bsonxD = []bsonx.Elem{bsonx.Elem{"hash", bsonx.Int32(1)}}
		indexModels = append(indexModels, mongo.IndexModel{
			Keys: bsonxD,
		})

		bsonxD = []bsonx.Elem{bsonx.Elem{"from", bsonx.Int32(1)}}
		indexModels = append(indexModels, mongo.IndexModel{
			Keys: bsonxD,
		})

		bsonxD = []bsonx.Elem{bsonx.Elem{"to", bsonx.Int32(1)}}
		indexModels = append(indexModels, mongo.IndexModel{
			Keys: bsonxD,
		})

		indexView := Txcoll.Indexes()
		_, err := indexView.CreateMany(context.Background(), indexModels)
		if err != nil {
			return err
		}

		fmt.Printf("Create txColl index time used: %v\n", time.Now().Sub(start))

		fmt.Printf("create blockColl index ...., now time is %v\n", time.Now())
		start = time.Now()

		var blockIndexModels []mongo.IndexModel
		bsonxD = []bsonx.Elem{bsonx.Elem{"writeTime", bsonx.Int32(1)}}
		blockIndexModels = append(blockIndexModels, mongo.IndexModel{
			Keys: bsonxD,
		})

		indexView = Blockcoll.Indexes()
		_, err = indexView.CreateMany(context.Background(), blockIndexModels)
		if err != nil {
			return err
		}

		fmt.Printf("Createb lockColl index time used: %v\n", time.Now().Sub(start))
	}

	for bindex := start; bindex <= blocknum; bindex++ {
		txs := make([]string, txnum)
		for tindex := 1; tindex <= txnum; tindex++ {
			TxHashByte, FromHashByte, ToHashByte := generateTx()
			T := TransactionRetrievalDoc4{
				TxHash:      "0x" + string(TxHashByte),
				TxIndex:     int64(tindex - 1),
				From:        "0x" + string(FromHashByte),
				To:          "0x" + string(ToHashByte),
				Extra:       "hello, world",
				BlockNumber: uint64(bindex),
			}
			_, err := Txcoll.InsertOne(nil, T)
			if err != nil {
				fmt.Printf("[S3] insert tx error, now number is %v, err: %v\n", bindex, err.Error())
				continue
			}
			txs[tindex-1] = T.TxHash
		}

		B := BlockRetrievalDoc45{
			BlockNumber:    uint64(bindex),
			BlockWriteTime: time.Now().UnixNano(),
			Txs:            txs,
		}
		_, err := Blockcoll.InsertOne(nil, B)
		if err != nil {
			fmt.Printf("[S3] insert block error, now number is %v, err: %v\n", bindex, err.Error())
			continue
		}
	}
	fmt.Printf("finish SolutionFour, time used: %v\n", time.Now().Sub(now))
	return nil
}

func SolutionFive(Txcoll *mongo.Collection, Blockcoll *mongo.Collection) error {
	fmt.Printf("---- start insert SolutionFive data... createIndex = %v\n----", createIndex)
	now := time.Now()
	if createIndex {
		fmt.Printf("create txColl index ...., now time is %v\n", time.Now())
		start := time.Now()

		// 创建非组合索引
		var indexModels []mongo.IndexModel
		var bsonxD bsonx.Doc
		bsonxD = []bsonx.Elem{bsonx.Elem{"from", bsonx.Int32(1)}}
		indexModels = append(indexModels, mongo.IndexModel{
			Keys: bsonxD,
		})

		bsonxD = []bsonx.Elem{bsonx.Elem{"to", bsonx.Int32(1)}}
		indexModels = append(indexModels, mongo.IndexModel{
			Keys: bsonxD,
		})

		indexView := Txcoll.Indexes()
		_, err := indexView.CreateMany(context.Background(), indexModels)
		if err != nil {
			return err
		}

		fmt.Printf("Create txColl index time used: %v\n", time.Now().Sub(start))

		fmt.Printf("create blockColl index ...., now time is %v\n", time.Now())
		start = time.Now()

		var blockIndexModels []mongo.IndexModel
		bsonxD = []bsonx.Elem{bsonx.Elem{"writeTime", bsonx.Int32(1)}}
		blockIndexModels = append(blockIndexModels, mongo.IndexModel{
			Keys: bsonxD,
		})

		indexView = Blockcoll.Indexes()
		_, err = indexView.CreateMany(context.Background(), blockIndexModels)
		if err != nil {
			return err
		}

		fmt.Printf("Createb lockColl index time used: %v\n", time.Now().Sub(start))
	}

	for bindex := start; bindex <= blocknum; bindex++ {
		txs := make([]string, txnum)
		for tindex := 1; tindex <= txnum; tindex++ {
			TxHashByte, FromHashByte, ToHashByte := generateTx()
			T := TransactionRetrievalDoc5{
				TxHash:      "0x" + string(TxHashByte),
				TxIndex:     int64(tindex - 1),
				From:        "0x" + string(FromHashByte),
				To:          "0x" + string(ToHashByte),
				Extra:       "hello, world",
				BlockNumber: uint64(bindex),
			}
			_, err := Txcoll.InsertOne(nil, T)
			if err != nil {
				fmt.Printf("[S3] insert tx error, now number is %v, err: %v\n", bindex, err.Error())
				continue
			}
			txs[tindex-1] = T.TxHash
		}

		B := BlockRetrievalDoc45{
			BlockNumber:    uint64(bindex),
			BlockWriteTime: time.Now().UnixNano(),
			Txs:            txs,
		}
		_, err := Blockcoll.InsertOne(nil, B)
		if err != nil {
			fmt.Printf("[S3] insert block error, now number is %v, err: %v\n", bindex, err.Error())
			continue
		}
	}
	fmt.Printf("finish SolutionFive, time used: %v\n", time.Now().Sub(now))
	return nil
}

func main() {
	cli, err := getDB()
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	// 方案一：嵌套
	coll := cli.Database("flato1").Collection(SolutionOneBlocks)
	err = SolutionOne(coll)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	time.Sleep(1 * time.Minute)

	// 方案二：引用，_id不为交易哈希
	Txcoll := cli.Database("flato2").Collection(SolutionTwoTransactions)
	Blockcoll := cli.Database("flato2").Collection(SolutionTwoBlocks)
	err = SolutionTwo(Txcoll, Blockcoll)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	time.Sleep(1 * time.Minute)

	// 方案三：引用，_id为交易哈希
	Txcoll3 := cli.Database("flato3").Collection(SolutionThreeTransactions)
	Blockcoll3 := cli.Database("flato3").Collection(SolutionThreeBlocks)
	err = SolutionThree(Txcoll3, Blockcoll3)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	time.Sleep(1 * time.Minute)

	// 方案四：引用，_id不为交易哈希
	Txcoll4 := cli.Database("flato4").Collection(SolutionFourTransactions)
	Blockcoll4 := cli.Database("flato4").Collection(SolutionFourBlocks)
	err = SolutionFour(Txcoll4, Blockcoll4)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	time.Sleep(1 * time.Minute)

	// 方案五：引用，_id为交易哈希
	Txcoll5 := cli.Database("flato5").Collection(SolutionFiveTransactions)
	Blockcoll5 := cli.Database("flato5").Collection(SolutionFiveBlocks)
	err = SolutionFive(Txcoll5, Blockcoll5)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	time.Sleep(1 * time.Minute)

}

var letters = []byte("1234567890abcdef")

// RandBytes fills the bytes with alphabetic characters randomly
func RandBytes(b []byte) {
	for i := range b {
		bi := new(big.Int).SetInt64(int64(len(letters)))
		index, _ := rand.Int(rand.Reader, bi)
		b[i] = letters[index.Int64()]
	}
}
