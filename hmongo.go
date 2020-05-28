package hmongo

import (
	"context"
	"fmt"
	"math"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

const (
	batchSize = 4096
)

type Index struct {
	DB     string
	Table  string
	Keys   []string
	Unique bool
}

type Option struct {
	Url         string `json:"url"`
	DBName      string `json:"db_name"`
	User        string `json:"user"`
	Password    string `json:"password"`
	MaxPoolSize uint64 `json:"max_pool_size"`
}

type MClient struct {
	DB     *mongo.Database
	Option *Option
}

func makeIndex(c *mongo.Collection, index *Index) error {
	var (
		indexOpts  *options.IndexOptions
		indexModel mongo.IndexModel
		indexKeys  bsonx.Doc
		err        error
	)

	indexOpts = &options.IndexOptions{}
	indexOpts.SetBackground(true)
	indexOpts.SetSparse(true)
	indexOpts.SetUnique(index.Unique)

	indexKeys = bsonx.Doc{}
	for _, v := range index.Keys {
		indexKeys = append(indexKeys, bsonx.Elem{Key: v, Value: bsonx.Int32(1)})
	}

	indexModel = mongo.IndexModel{Keys: indexKeys, Options: indexOpts}

	_, err = c.Indexes().CreateOne(context.Background(), indexModel, options.CreateIndexes())
	if err != nil {
		return fmt.Errorf("create index=%v for col=%v failed: %v\n", index.Keys, index.Table, err)
	} else {
		fmt.Printf("create index=%v for col=%v succ.\n", index.Keys, index.Table)
	}

	return nil
}

func Init(option *Option) (*MClient, error) {

	opts := options.ClientOptions{}
	if len(option.User) > 0 {
		opts.SetAuth(options.Credential{
			AuthMechanism: "SCRAM-SHA-1",
			AuthSource:    option.DBName,
			Username:      option.User,
			Password:      option.Password,
		})
	}

	opts.ApplyURI(option.Url)
	opts.SetHeartbeatInterval(3 * time.Second)
	opts.SetSocketTimeout(3 * time.Second)
	opts.SetConnectTimeout(3 * time.Second)
	opts.SetMaxPoolSize(option.MaxPoolSize)

	client, err := mongo.Connect(context.Background(), &opts)
	if err != nil {
		return nil, fmt.Errorf("connect mongo server with official sdk failed: %v\n", err)
	}

	hClient := &MClient{DB: client.Database(option.DBName), Option: option}
	return hClient, nil
}

func (m *MClient) MakeIndex(colName string, keys []string, unique bool) {
	index := mongo.IndexModel{}
	indexKeys := bsonx.Doc{}
	for _, v := range keys {
		indexKeys = append(indexKeys, bsonx.Elem{Key: v, Value: bsonx.Int32(1)})
	}
	index.Keys = indexKeys

	indexOpts := &options.IndexOptions{}
	indexOpts.SetBackground(true)
	indexOpts.SetSparse(true)
	indexOpts.SetUnique(unique)
	index.Options = indexOpts

	col := m.DB.Collection(colName)
	_, err := col.Indexes().CreateOne(context.Background(), index, options.CreateIndexes())
	if nil != err {
		fmt.Printf("create index=%v for col=%v failed: %v\n", keys, colName, err)
	} else {
		fmt.Printf("create index=%v for col=%v succ.\n", keys, colName)
	}
}

// error, not found
func (m *MClient) LoadMongoDBOne(col_name string, query, record interface{}) (error, bool) {
	col := m.DB.Collection(col_name)
	err := col.FindOne(context.Background(), query).Decode(record)
	if nil != err {
		if mongo.ErrNoDocuments != err {
			return err, false
		}
		return err, true
	}
	return nil, false
}

func (m *MClient) LoadMongoDBAll(col_name string, query interface{}, f func(cur *mongo.Cursor)) {
	col := m.DB.Collection(col_name)
	ctx := context.Background()

	total, _ := col.CountDocuments(ctx, query)
	query_cnt := math.Ceil(float64(total) / float64(batchSize))

	for i := 0; i < int(query_cnt); i++ {
		opts := options.Find()
		opts.SetSkip(int64(i) * batchSize)
		opts.SetLimit(batchSize)

		cur, err := col.Find(ctx, query, opts)
		if nil != err {
			fmt.Printf("load mongo all record failed: %v, col=%v, query=%v\n", err, col_name, query)
			continue
		}

		for cur.Next(ctx) {
			// var obj *type
			// cur.Decode(obj), then handle obj
			f(cur)
		}
		cur.Close(ctx)
	}
}

func (m *MClient) InsertMongoDBOne(col_name string, record interface{}) (*mongo.InsertOneResult, error) {
	col := m.DB.Collection(col_name)
	return col.InsertOne(context.Background(), record)
}

func (m *MClient) InsertMongoDBMany(col_name string, records []interface{}) (*mongo.InsertManyResult, error) {
	col := m.DB.Collection(col_name)
	return col.InsertMany(context.Background(), records)
}

func (m *MClient) UpdateMongoDB(col_name string, selector, update interface{}) (*mongo.UpdateResult, error) {
	col := m.DB.Collection(col_name)
	return col.UpdateMany(context.Background(), selector, update)
}

func (m *MClient) DeleteMongoDB(col_name string, selector interface{}) (*mongo.DeleteResult, error) {
	col := m.DB.Collection(col_name)
	return col.DeleteOne(context.Background(), selector)
}

func (m *MClient) UpsertMongoDBOne(col_name string, selector, record interface{}) (*mongo.UpdateResult, error) {
	col := m.DB.Collection(col_name)
	upsert := true
	opt := options.ReplaceOptions{Upsert: &upsert}
	return col.ReplaceOne(context.Background(), selector, record, &opt)
}

func (m *MClient) LoadMongoByPage(col_name string, query, sortFields interface{}, skip, limit int64, f func(cur *mongo.Cursor)) int64 {
	col := m.DB.Collection(col_name)
	ctx := context.Background()

	total, _ := col.CountDocuments(ctx, query)

	opts := options.Find()
	opts.SetSkip(skip)
	opts.SetLimit(limit)

	sort := options.Find().SetSort(sortFields)

	cur, err := col.Find(ctx, query, opts, sort)
	if nil != err {
		fmt.Printf("load mongo all record failed: %v, col=%v, query=%v\n", err, col_name, query)
		return 0
	}

	for cur.Next(ctx) {
		// var obj *type
		// cur.Decode(obj), then handle obj
		f(cur)
	}
	cur.Close(ctx)
	return total
}
