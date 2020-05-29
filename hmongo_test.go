package hmongo

import (
	"fmt"
	"testing"
)

type User struct {
	Username string
	Name     string
	Age      int
	Address  string
}

const MongoUrl = "mongodb://127.0.0.1:27017"

func TestMClient_InsertOne(t *testing.T) {
	if err := Init(&Config{Url: MongoUrl, MaxPoolSize: 5}); err != nil {
		t.Error(err)
		return
	}

	user := &User{Username: "changsan", Name: "张三", Age: 18, Address: "广州"}

	m := New("test", "user")
	if _, err := m.InsertOne(user); err != nil {
		t.Error(err)
	}
}

func TestMClient_InsertMany(t *testing.T) {

	user := []interface{}{
		&User{Username: "zhangsan", Name: "张三", Age: 18, Address: "北京"},
		&User{Username: "lisi", Name: "李四", Age: 28, Address: "上海"},
		&User{Username: "wangwu", Name: "王五", Age: 38, Address: "北京"},
		&User{Username: "mutouliu", Name: "木头六", Age: 48, Address: "深圳"},
	}

	if err := Init(&Config{Url: MongoUrl, MaxPoolSize: 5}); err != nil {
		t.Error(err)
		return
	}

	m := New("test", "user")
	if _, err := m.InsertMany(user); err != nil {
		t.Error(err)
	}
}

func TestMClient_Save(t *testing.T) {

	user := &User{Username: "hehe", Name: "呵呵", Age: 88, Address: "郑州"}

	if err := Init(&Config{Url: MongoUrl, MaxPoolSize: 5}); err != nil {
		t.Error(err)
		return
	}

	m := New("test", "user")
	if _, err := m.InsertOrReplace(M{"username": "hehe"}, user); err != nil {
		t.Error(err)
	}
}

func TestMClient_QueryOne(t *testing.T) {
	if err := Init(&Config{Url: MongoUrl, MaxPoolSize: 5}); err != nil {
		t.Error(err)
		return
	}

	m := New("test", "user")
	var user User
	if err := m.QueryOne(M{"username": "zhangsan"}, nil, &user); err != nil {
		t.Error(err)
		return
	}
	fmt.Println(user)

	var user2 User
	if err := m.QueryOne(M{"username": "zhangsan"}, M{"username": 1}, &user2); err != nil {
		t.Error(err)
		return
	}
	fmt.Println(user2)

}

func TestMClient_QueryByCursor(t *testing.T) {
	if err := Init(&Config{Url: MongoUrl, MaxPoolSize: 5}); err != nil {
		t.Error(err)
		return
	}

	m := New("test", "user")

	var users []*User

	f := func(hc *HCursor) {
		var user User
		if err := hc.Cursor.Decode(&user); err != nil {
			t.Error(err)
			return
		}
		users = append(users, &user)
	}

	if err := m.QueryByCursor(M{}, nil, nil, f); err != nil {
		t.Error(err)
		return
	}

	for _, user := range users {
		fmt.Printf("%+v\n", user)
	}
}

func TestMClient_QueryAll(t *testing.T) {
	if err := Init(&Config{Url: MongoUrl, MaxPoolSize: 5}); err != nil {
		t.Error(err)
		return
	}

	m := New("test", "user")

	var users []*User

	if err := m.QueryAll(M{}, nil, nil, &users); err != nil {
		t.Error(err)
		return
	}

	for _, user := range users {
		fmt.Printf("%+v\n", user)
	}
}

func TestMClient_QueryWithPage(t *testing.T) {
	if err := Init(&Config{Url: MongoUrl, MaxPoolSize: 5}); err != nil {
		t.Error(err)
		return
	}

	m := New("test", "user")

	var users []*User

	f := func(hc *HCursor) {
		var user User
		if err := hc.Cursor.Decode(&user); err != nil {
			t.Error(err)
			return
		}
		users = append(users, &user)
	}

	page, err := m.QueryWithPage(M{}, nil, M{"age": -1, "username": 1}, 1, 5, f)
	if err != nil {
		t.Error(err)
		return
	}

	for _, user := range users {
		fmt.Printf("%+v\n", user)
	}

	fmt.Printf("%+v\n", page)

}

func TestMClient_UpdateOne(t *testing.T) {
	if err := Init(&Config{Url: MongoUrl, MaxPoolSize: 5}); err != nil {
		t.Error(err)
		return
	}

	m := New("test", "user")
	r, err := m.UpdateOne(M{"username": "changsan"}, M{"$set": M{"name": "长三"}})
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Printf("%+v\n", r)
}

func TestMClient_UpdateMany(t *testing.T) {
	if err := Init(&Config{Url: MongoUrl, MaxPoolSize: 5}); err != nil {
		t.Error(err)
		return
	}

	m := New("test", "user")
	r, err := m.UpdateMany(M{"address": "北京"}, M{"$set": M{"address": "帝都"}})
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Printf("%+v\n", r)
}

func TestMClient_DeleteOne(t *testing.T) {
	if err := Init(&Config{Url: MongoUrl, MaxPoolSize: 5}); err != nil {
		t.Error(err)
		return
	}
	m := New("test", "user")
	r, err := m.DeleteOne(M{"username": "changsan"})
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Printf("%+v\n", r)
}

func TestMClient_DeleteMany(t *testing.T) {
	if err := Init(&Config{Url: MongoUrl, MaxPoolSize: 5}); err != nil {
		t.Error(err)
		return
	}
	m := New("test", "user")
	r, err := m.DeleteMany(M{"username": "changsan"})
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Printf("%+v\n", r)
}

func TestInit_MakeIndex(t *testing.T) {
	indexes := []Index{
		{DB: "test", Collection: "user", Keys: []string{"username"}, Unique: true},
	}

	if err := Init(&Config{Url: MongoUrl, MaxPoolSize: 5}, indexes...); err != nil {
		t.Error(err)
		return
	}
}
