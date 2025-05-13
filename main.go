package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/scrapeless-ai/scrapeless-actor-sdk-go/scrapeless"
	"github.com/scrapeless-ai/scrapeless-actor-sdk-go/scrapeless/httpserver"
	"github.com/scrapeless-ai/scrapeless-actor-sdk-go/scrapeless/log"
	"github.com/scrapeless-ai/scrapeless-actor-sdk-go/scrapeless/storage/queue"
	"math/rand"
	"time"
)

type RequestParam struct {
	Name string `json:"name"`
}

var (
	actor *scrapeless.Actor
)

func main() {
	actor = scrapeless.New(scrapeless.WithStorage(), scrapeless.WithServer())
	defer actor.Close()
	var param = &RequestParam{}
	if err := actor.Input(param); err != nil {
		log.GetLogger().Error().Msg(err.Error())
		return
	}
	test([]byte(`{"name":"test"}`))
	for i := 0; i < 10; i++ {
		msgId, err := actor.Storage.GetQueue().Push(context.Background(), queue.PushQueue{
			Name:    fmt.Sprintf("%d", i),
			Payload: []byte(fmt.Sprintf(`{"name":"%s-%d"}`, param.Name, i)),
		})
		if err != nil {
			log.GetLogger().Error().Msg(err.Error())
		}
		log.GetLogger().Info().Msgf("push msgId:%s\n", msgId)
		pullResp, err := actor.Storage.GetQueue().Pull(context.Background(), 1)
		if err != nil {
			log.GetLogger().Error().Msg(err.Error())
		}
		log.GetLogger().Info().Msgf("get msgId %s,msg:%+v\n", pullResp[0].ID, pullResp)
		err = actor.Storage.GetQueue().Ack(context.Background(), msgId)
		if err != nil {
			log.GetLogger().Error().Msg(err.Error())
		}
		log.GetLogger().Info().Msgf("ack msgId:%s\n", msgId)
	}
	actor.Server.AddHandle("/test", test)
	go actor.Start()
	for {
		randLog()
		time.Sleep(time.Second)
	}
}

func test(t []byte) (httpserver.Response, error) {
	var param = &RequestParam{}
	json.Unmarshal(t, param)
	for i := 0; i < 5; i++ {
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
		log.GetLogger().Info().Msgf("add items: %+v\n", items)
		_, err := actor.Storage.GetDataset().AddItems(context.Background(), items)
		if err != nil {
			log.GetLogger().Error().Msg(err.Error())
		}
		getItems, err := actor.Storage.GetDataset().GetItems(context.Background(), 1, 10, false)
		if err != nil {
			log.GetLogger().Error().Msg(err.Error())
		}
		log.GetLogger().Info().Msgf("get items: %+v\n", getItems)
		time.Sleep(time.Second)
	}
	log.GetLogger().Info().Msg("add items success")

	for i := 0; i < 5; i++ {
		_, err := actor.Storage.GetKv().SetValue(
			context.Background(),
			fmt.Sprintf("key-%d", i),
			fmt.Sprintf("value-%d", i),
			0)
		if err != nil {
			log.GetLogger().Error().Msgf("set value err:%v\n", err)
		}
		value, err := actor.Storage.GetKv().GetValue(context.Background(), fmt.Sprintf("key-%d", i))
		if err != nil {
			log.GetLogger().Error().Msgf("get value err:%v\n", err)
		}
		log.GetLogger().Info().Msgf("get value: %s\n", value)
		time.Sleep(time.Second)
	}
	log.GetLogger().Info().Msg("add items success !")
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
		log.GetLogger().Trace().Msg("This is a trace log")
	case 1:
		// 模拟输出Debug级别日志
		log.GetLogger().Debug().Msg("This is a debug log")
	case 2:
		// 模拟输出Info级别日志
		log.GetLogger().Log().Msg("This is an info log")
	case 3:
		// 模拟输出Warn级别日志
		log.GetLogger().Warn().Msg("This is a warning log")
	case 4:
		// 模拟输出Error级别日志
		log.GetLogger().Error().Msg("This is an error log")
	default:
		// 模拟输出Info级别日志
		log.GetLogger().Info().Msg("This is an info log")
	}
}
