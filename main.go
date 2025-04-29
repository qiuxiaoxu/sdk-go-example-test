package main

import (
	"context"
	"fmt"
	"github.com/scrapeless-ai/scrapeless-actor-sdk-go/scrapeless"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"time"
)

func init() {
	log.SetFormatter(&log.TextFormatter{})
}

type RequestParam struct {
	Name string `json:"name"`
}

func main() {
	actor := scrapeless.New(scrapeless.WithStorage())
	defer actor.Close()
	var param = &RequestParam{}
	if err := actor.Input(param); err != nil {
		log.Error(err)
		return
	}

	// 模拟向队Dataset添加数据
	for i := 0; i < 30; i++ {
		items := []map[string]any{
			{
				"name": fmt.Sprintf("name-%d", i),
				"age":  i * 10,
				"sex":  i%2 == 0,
				"info": map[string]any{
					"name": fmt.Sprintf("name-%d", i),
					"age":  i * 10,
					"sex":  i%2 == 0,
				},
				"uuid": fmt.Sprintf("uuid-%d", i),
				"time": time.Now().Unix(),
			},
		}
		log.Debugf("add items: %+v", items)
		_, err := actor.Storage.GetDataset().AddItems(context.Background(), items)
		if err != nil {
			log.Error(err)
		}
		time.Sleep(time.Second)
	}
	log.Info("add items success")

	// 模拟向KV添加数据
	for i := 0; i < 30; i++ {
		_, err := actor.Storage.GetKv().SetValue(
			context.Background(),
			fmt.Sprintf("key-%d", i),
			fmt.Sprintf("value-%d", i),
			0)
		if err != nil {
			log.Error("set value err:", err)
		}
		time.Sleep(time.Second)
	}

	log.Infof("add items success")
	put, err := actor.Storage.GetObject().Put(context.Background(), "key.json", []byte(fmt.Sprintf(`{"name":"%s"}`, param.Name)))
	if err != nil {
		panic(err)
	}
	log.Println("Dataset AddItems,", put)
	for {
		// 模拟运行，每秒钟随机输出一些运行日志
		randLog()
		time.Sleep(time.Second)
	}
}

// randLog 随机输出一些运行日志
func randLog() {
	logLevel := 0
	logLevel = rand.Intn(5)
	switch logLevel {
	case 0:
		// 模拟输出Trace级别日志
		log.Trace("This is a trace log")
	case 1:
		// 模拟输出Debug级别日志
		log.Debug("This is a debug log")
	case 2:
		// 模拟输出Info级别日志
		log.Info("This is an info log")
	case 3:
		// 模拟输出Warn级别日志
		log.Warn("This is a warning log")
	case 4:
		// 模拟输出Error级别日志
		log.Error("This is an error log")
	default:
		// 模拟输出Info级别日志
		log.Info("This is an info log")
	}
}
