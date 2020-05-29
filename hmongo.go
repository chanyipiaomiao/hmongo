package hmongo

import (
	"context"
	"errors"
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

var (
	mongoClient      *mongo.Client
	ErrorNoDocsFound = errors.New("no docs found")
)

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
		return fmt.Errorf("create index=%v for col=%v failed: %v\n", index.Keys, index.Collection, err)
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

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	mongoClient, err = mongo.Connect(ctx, &clientOptions)
	if err != nil {
		return fmt.Errorf("connect mongo server with official sdk failed: %v\n", err)
	}

	for _, index := range indexes {
		c := mongoClient.Database(index.DB).Collection(index.Collection)
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

func (m *MClient) Disconnect() error {
	return m.client.Disconnect(context.Background())
}

// 根据过滤条件 不存在时插入，存在时替换
func (m *MClient) InsertOrReplace(filter, doc interface{}) (*UpdateResult, error) {
	var (
		err          error
		updateResult *mongo.UpdateResult
		upSert       = true
		opt          = &options.ReplaceOptions{Upsert: &upSert}
	)
	if updateResult, err = m.coll.ReplaceOne(context.Background(), filter, doc, opt); err != nil {
		return nil, err
	}

	return &UpdateResult{
		MatchedCount:  updateResult.MatchedCount,
		ModifiedCount: updateResult.ModifiedCount,
		UpsertedCount: updateResult.UpsertedCount,
		UpsertedID:    updateResult.UpsertedID,
	}, nil
}

func (m *MClient) InsertOne(doc interface{}) (*InsertOneResult, error) {
	var (
		err          error
		insertResult *mongo.InsertOneResult
	)

	if insertResult, err = m.coll.InsertOne(context.Background(), doc); err != nil {
		return nil, err
	}
	return &InsertOneResult{InsertedID: insertResult.InsertedID}, nil
}

func (m *MClient) InsertMany(docs []interface{}) (*InsertManyResult, error) {
	var (
		err          error
		insertResult *mongo.InsertManyResult
	)

	if insertResult, err = m.coll.InsertMany(context.Background(), docs); err != nil {
		return nil, err
	}
	return &InsertManyResult{InsertedIDs: insertResult.InsertedIDs}, nil
}

func (m *MClient) QueryOne(filter, projection, docs interface{}) error {
	option := &options.FindOneOptions{Projection: projection}
	if err := m.coll.FindOne(context.Background(), filter, option).Decode(docs); err != nil {
		return err
	}
	return nil
}

func (m *MClient) QueryByCursor(filter, projection, sortFields interface{}, f func(hc *HCursor)) error {
	var (
		option     *options.FindOptions
		cursor     *mongo.Cursor
		err        error
		ctx        = context.Background()
		total      int64
		queryCount float64
	)

	if total, err = m.coll.CountDocuments(ctx, filter); err != nil {
		return err
	}

	if total == 0 {
		return ErrorNoDocsFound
	}

	queryCount = math.Ceil(float64(total) / float64(batchSize))

	for i := 0; i < int(queryCount); i++ {

		option = &options.FindOptions{Projection: projection}
		option.SetSkip(int64(i) * batchSize)
		option.SetLimit(batchSize)
		option.SetSort(sortFields)

		if cursor, err = m.coll.Find(ctx, filter, option); err != nil {
			return err
		}

		for cursor.Next(ctx) {
			f(&HCursor{Cursor: cursor})
		}

		if err = cursor.Close(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (m *MClient) QueryAll(filter, projection, sortFields, records interface{}) error {
	var (
		err        error
		ctx        = context.Background()
		option     *options.FindOptions
		cursor     *mongo.Cursor
		total      int64
		queryCount float64
	)

	if total, err = m.coll.CountDocuments(ctx, filter); err != nil {
		return err
	}

	if total == 0 {
		return ErrorNoDocsFound
	}

	queryCount = math.Ceil(float64(total) / float64(batchSize))

	for i := 0; i < int(queryCount); i++ {
		option = &options.FindOptions{Projection: projection}
		option.SetSkip(int64(i) * batchSize)
		option.SetLimit(batchSize)
		option.SetSort(sortFields)

		if cursor, err = m.coll.Find(ctx, filter, option); err != nil {
			return err
		}

		if err = cursor.All(ctx, records); err != nil {
			return err
		}
	}

	return nil
}

func (m *MClient) QueryWithPage(filter, projection, sortFields interface{},
	pageNo, pageSize int64, f func(hc *HCursor)) (*Page, error) {
	var (
		ctx    = context.Background()
		total  int64
		option *options.FindOptions
		err    error
		cursor *mongo.Cursor
		page   *Page
		skip   = pageNo - 1
	)

	total, _ = m.coll.CountDocuments(ctx, filter)
	if total == 0 {
		return nil, ErrorNoDocsFound
	}

	option = &options.FindOptions{
		Projection: projection,
		Skip:       &skip,
		Limit:      &pageSize,
		Sort:       sortFields,
	}

	if cursor, err = m.coll.Find(ctx, filter, option); err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		f(&HCursor{Cursor: cursor})
	}

	page = PageUtil(total, pageNo, pageSize)

	return page, nil
}

func (m *MClient) UpdateOne(filter, update interface{}) (*UpdateResult, error) {
	var (
		err          error
		ctx          = context.Background()
		updateResult *mongo.UpdateResult
	)

	if updateResult, err = m.coll.UpdateOne(ctx, filter, update); err != nil {
		return nil, err
	}

	return &UpdateResult{
		MatchedCount:  updateResult.MatchedCount,
		ModifiedCount: updateResult.ModifiedCount,
		UpsertedCount: updateResult.UpsertedCount,
		UpsertedID:    updateResult.UpsertedID,
	}, nil
}

func (m *MClient) UpdateMany(filter, update interface{}) (*UpdateResult, error) {
	var (
		err          error
		ctx          = context.Background()
		updateResult *mongo.UpdateResult
	)

	if updateResult, err = m.coll.UpdateMany(ctx, filter, update); err != nil {
		return nil, err
	}

	return &UpdateResult{
		MatchedCount:  updateResult.MatchedCount,
		ModifiedCount: updateResult.ModifiedCount,
		UpsertedCount: updateResult.UpsertedCount,
		UpsertedID:    updateResult.UpsertedID,
	}, nil
}

func (m *MClient) DeleteOne(filter interface{}) (*DeleteResult, error) {
	var (
		err          error
		ctx          = context.Background()
		deleteResult *mongo.DeleteResult
	)

	if deleteResult, err = m.coll.DeleteOne(ctx, filter); err != nil {
		return nil, err
	}

	return &DeleteResult{DeletedCount: deleteResult.DeletedCount}, nil
}

func (m *MClient) DeleteMany(filter interface{}) (*DeleteResult, error) {
	var (
		err          error
		ctx          = context.Background()
		deleteResult *mongo.DeleteResult
	)

	if deleteResult, err = m.coll.DeleteMany(ctx, filter); err != nil {
		return nil, err
	}

	return &DeleteResult{DeletedCount: deleteResult.DeletedCount}, nil
}
