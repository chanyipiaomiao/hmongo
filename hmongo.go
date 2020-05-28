package hmongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

const (
	batchSize = 4096
)

var (
	mongoClient *mongo.Client
)

type M map[string]interface{}

type Index struct {
	DB     string
	Table  string
	Keys   []string
	Unique bool
}

type Config struct {
	Url         string `json:"url"`
	DBName      string `json:"db_name"`
	User        string `json:"user"`
	Password    string `json:"password"`
	MaxPoolSize uint64 `json:"max_pool_size"`
}

type MClient struct {
	client *mongo.Client
	coll   *mongo.Collection
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
	}

	return nil
}

func Init(cfg *Config, indexes ...Index) error {
	var (
		clientOptions options.ClientOptions
		err           error
	)

	clientOptions = options.ClientOptions{}
	if len(cfg.User) > 0 {
		clientOptions.SetAuth(options.Credential{
			AuthMechanism: "SCRAM-SHA-1",
			AuthSource:    cfg.DBName,
			Username:      cfg.User,
			Password:      cfg.Password,
		})
	}

	clientOptions.ApplyURI(cfg.Url)
	clientOptions.SetHeartbeatInterval(3 * time.Second)
	clientOptions.SetSocketTimeout(3 * time.Second)
	clientOptions.SetConnectTimeout(3 * time.Second)
	clientOptions.SetMaxPoolSize(cfg.MaxPoolSize)

	mongoClient, err = mongo.Connect(context.Background(), &clientOptions)
	if err != nil {
		return fmt.Errorf("connect mongo server with official sdk failed: %v\n", err)
	}

	for _, index := range indexes {
		c := mongoClient.Database(index.DB).Collection(index.Table)
		if err = makeIndex(c, &index); err != nil {
			return err
		}
	}

	return nil
}

func New(db, coll string) *MClient {
	return &MClient{
		client: mongoClient,
		coll:   mongoClient.Database(db).Collection(coll),
	}
}

func (m *MClient) InsertOne(doc interface{}) error {
	if _, err := m.coll.InsertOne(context.Background(), doc); err != nil {
		return err
	}
	return nil
}

func (m *MClient) InsertMany(docs []interface{}) error {
	if _, err := m.coll.InsertMany(context.Background(), docs); err != nil {
		return err
	}
	return nil
}

func (m *MClient) QueryOne(filter, projection, record interface{}) error {
	option := &options.FindOneOptions{Projection: projection}
	err := m.coll.FindOne(context.Background(), filter, option).Decode(record)
	if err != nil {
		return err
	}
	return nil
}
