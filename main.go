package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/scrapeless-ai/scrapeless-actor-sdk-go/env"
	"github.com/scrapeless-ai/scrapeless-actor-sdk-go/scrapeless"
	"github.com/scrapeless-ai/scrapeless-actor-sdk-go/scrapeless/httpserver"
	"github.com/scrapeless-ai/scrapeless-actor-sdk-go/scrapeless/log"
	"github.com/scrapeless-ai/scrapeless-actor-sdk-go/scrapeless/storage/queue"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"
)

type RequestParam struct {
	Name    string `json:"name"`
	Keyword string `json:"keyword"`
}

var (
	actor *scrapeless.Actor
)

func main() {
	_, err := os.Create("/var/scrapeless/9a87fbdc-759f-489b-9173-2f0bf32b159b/1.txt")
	if err != nil {
		log.Error(err.Error())
	}

	actor = scrapeless.New(scrapeless.WithStorage(), scrapeless.WithServer())
	defer actor.Close()
	var param = &RequestParam{}
	if err := actor.Input(param); err != nil {
		log.Error(err.Error())
		return
	}
	_, err = os.Create(fmt.Sprintf("/var/scrapeless/%s/1.txt", env.Env.Actor.RunId))
	if err != nil {
		log.Error(err.Error())
	}
	test([]byte(`{"name":"test"}`))
	for i := 0; i < 10; i++ {
		msgId, err := actor.Storage.GetQueue().Push(context.Background(), queue.PushQueue{
			Name:    fmt.Sprintf("%d", i),
			Payload: []byte(fmt.Sprintf(`{"name":"%s-%d"}`, param.Name, i)),
		})
		if err != nil {
			log.Error(err.Error())
		}
		log.Infof("push msgId:%s", msgId)
		pullResp, err := actor.Storage.GetQueue().Pull(context.Background(), 1)
		if err != nil {
			log.Error(err.Error())
		}
		log.Infof("get msgId %s,msg:%+v", pullResp[0].ID, pullResp)
		err = actor.Storage.GetQueue().Ack(context.Background(), msgId)
		if err != nil {
			log.Error(err.Error())
		}
		log.Infof("ack msgId:%s", msgId)
	}
	actor.Server.AddHandle("/test", test)
	data, err := actor.Router.Request(param.Keyword, http.MethodGet, "/hello", nil, nil)
	if err != nil {
		log.Error(err.Error())
	}
	log.Infof("get data:%s", string(data))
	go func() {
		if err := actor.Start(); err != nil {
			log.Error(err.Error())
		}
		log.Infof("actor start success at port : %s", env.Env.Actor.HttpPort)
	}()
	time.Sleep(time.Second * 10)
	request, err := actor.Router.Request(env.Env.Actor.RunId, http.MethodPost, "/test", strings.NewReader(`{"name":"cy"}`), map[string]string{
		"Content-Type": "application/json",
	})
	if err != nil {
		log.Error(err.Error())
	}
	log.Infof("get data:%s", string(request))
	for {
		randLog()
		time.Sleep(time.Second)
	}
}

func test(t []byte) (httpserver.Response, error) {
	var param = &RequestParam{}
	json.Unmarshal(t, param)
	for i := 0; i < 2; i++ {
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
		log.Infof("add items: %+v", items)
		_, err := actor.Storage.GetDataset().AddItems(context.Background(), items)
		if err != nil {
			log.Error(err.Error())
		}
		getItems, err := actor.Storage.GetDataset().GetItems(context.Background(), 1, 10, false)
		if err != nil {
			log.Error(err.Error())
		}
		log.Infof("get items: %+v", getItems)
		time.Sleep(time.Second)
	}
	log.Info("add items success")

	for i := 0; i < 2; i++ {
		_, err := actor.Storage.GetKv().SetValue(
			context.Background(),
			fmt.Sprintf("key-%d", i),
			fmt.Sprintf("value-%d", i),
			0)
		if err != nil {
			log.Errorf("set value err:%v", err)
		}
		value, err := actor.Storage.GetKv().GetValue(context.Background(), fmt.Sprintf("key-%d", i))
		if err != nil {
			log.Errorf("get value err:%v", err)
		}
		log.Infof("get value: %s", value)
		time.Sleep(time.Second)
	}
	log.Info("add items success !")
	//put, err := actor.Storage.GetObject().Put(context.Background(), "key.json", []byte(fmt.Sprintf(`{"name":"%s"}`, param.Name)))
	//if err != nil {
	//	panic(err)
	//}
	//get, err := actor.Storage.GetObject().Get(context.Background(), put)
	//if err != nil {
	//
	//}
	//log.Println("Dataset AddItems,", put)
	return httpserver.Response{
		Code: 0,
		Data: "success",
		Msg:  "success",
	}, nil
}

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
