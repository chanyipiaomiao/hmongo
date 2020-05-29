### hmongo

golang操作mongodb库，增删改查、创建索引

基于 [官方mongodb](https://github.com/mongodb/mongo-go-driver) 库封装的操作库

### 示例

#### 插入单个

```go
package main

import (
	"github.com/chanyipiaomiao/hmongo"
	"log"
)

const (
	MongoUrl = "mongodb://127.0.0.1:27017"
	MaxPoolSize = 5
)

type User struct {
	Username string
	Name     string
	Age      int
	Address  string
}

func main() {

	cfg := &hmongo.Config{
		Url: MongoUrl,
		MaxPoolSize: MaxPoolSize,
	}

	if err := hmongo.Init(cfg); err != nil {
		log.Fatal(err)
	}

	m := hmongo.New("test", "user")
	user := &User{Username: "zhangsan", Name: "张三", Age: 18, Address: "北京"}

	if _, err := m.InsertOne(user); err != nil {
		log.Fatal(err)
	}
}
```
必须首先调hmongo.Init进行初始化

#### 插入多个

```go
user := []interface{}{
    &User{Username: "zhangsan", Name: "张三", Age: 18, Address: "北京"},
    &User{Username: "lisi", Name: "李四", Age: 28, Address: "上海"},
    &User{Username: "wangwu", Name: "王五", Age: 38, Address: "北京"},
    &User{Username: "mutouliu", Name: "木头六", Age: 48, Address: "深圳"},
}

if _, err := m.InsertMany(user); err != nil {
    log.Fatal(err)
}
```

#### 插入or替换

当过滤条件过滤文档时，如果要过滤不存在，插入，存在时 替换现有的文档所有字段

```go
user := &User{Username: "hehe", Name: "呵呵", Age: 68, Address: "郑州"}
if _, err := m.InsertOrReplace(hmongo.M{"username": "hehe"}, user); err != nil {
    log.Fatal(err)
}
```

#### 查询单个

```go
// 返回全部字段
var user User
if err := m.QueryOne(hmongo.M{"username": "zhangsan"}, nil, &user); err != nil {
    log.Fatal(err)
}
fmt.Println(user)

// 返回指定的字段
var user2 User
if err := m.QueryOne(hmongo.M{"username": "zhangsan"}, hmongo.M{"username": 1}, &user2); err != nil {
    log.Fatal(err)
}
fmt.Println(user2)
```

#### 查询所有

hmongo.M{} 指定过滤条件

```go
var users []*User

if err := m.QueryAll(hmongo.M{}, nil, nil, &users); err != nil {
    log.Fatal(err)
}

for _, user := range users {
    fmt.Printf("%+v\n", user)
}
```

#### 通过Cursor查询

```go
var users []*User

f := func(hc *hmongo.HCursor) {
    var user User
    if err := hc.Cursor.Decode(&user); err != nil {
        log.Fatal(err)
        return
    }
    users = append(users, &user)
}

if err := m.QueryByCursor(hmongo.M{}, nil, nil, f); err != nil {
    log.Fatal(err)
}

for _, user := range users {
    fmt.Printf("%+v\n", user)
}
```

#### 分页查询

hmongo.M{"age": -1, "username": 1}
可以指定排序字段 1: 正序 -1: 倒序

```go
var users []*User

f := func(hc *hmongo.HCursor) {
    var user User
    if err := hc.Cursor.Decode(&user); err != nil {
        log.Fatal(err)
    }
    users = append(users, &user)
}

page, err := m.QueryWithPage(hmongo.M{}, nil, hmongo.M{"age": -1, "username": 1}, 1, 5, f)
if err != nil {
    log.Fatal(err)
}

for _, user := range users {
    fmt.Printf("%+v\n", user)
}

fmt.Printf("%+v\n", page)
```

#### 更新单个

```go
r, err := m.UpdateOne(hmongo.M{"username": "hehe"}, hmongo.M{"$set": hmongo.M{"name": "呵呵呵"}})
if err != nil {
    log.Fatal(err)
}
fmt.Printf("%+v\n", r)
```

#### 更新多个

```go
r, err := m.UpdateMany(hmongo.M{"address": "北京"}, hmongo.M{"$set": hmongo.M{"address": "帝都"}})
if err != nil {
    log.Fatal(err)
}
fmt.Printf("%+v\n", r)
```

#### 删除单个

```go
r, err := m.DeleteOne(hmongo.M{"username" : "hehe"})
if err != nil {
    log.Fatal(err)
}
fmt.Printf("%+v\n", r)
```

#### 删除多个

```go
r, err := m.DeleteMany(hmongo.M{"username" : "mutouliu"})
if err != nil {
    log.Fatal(err)
}
fmt.Printf("%+v\n", r)
```

#### 创建索引

在初始化的时候创建索引

```go
indexes := []hmongo.Index{
    {DB: "test", Collection: "user", Keys: []string{"username"}, Unique: true},
}

if err := hmongo.Init(cfg, indexes...); err != nil {
    log.Fatal(err)
}
```