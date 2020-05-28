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

func TestMClient_InsertOne(t *testing.T) {
	if err := Init(&Config{Url: "mongodb://127.0.0.1:27017", MaxPoolSize: 5}); err != nil {
		t.Error(err)
		return
	}

	user := &User{
		Username: "zhangsan",
		Name:     "张三",
		Age:      18,
		Address:  "北京",
	}

	m := New("test", "user")
	if err := m.InsertOne(user); err != nil {
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

	if err := Init(&Config{Url: "mongodb://127.0.0.1:27017", MaxPoolSize: 5}); err != nil {
		t.Error(err)
		return
	}

	m := New("test", "user")
	if err := m.InsertMany(user); err != nil {
		t.Error(err)
	}
}

func TestMClient_QueryOne(t *testing.T) {
	if err := Init(&Config{Url: "mongodb://127.0.0.1:27017", MaxPoolSize: 5}); err != nil {
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
