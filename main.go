package main

import (
	"context"
	"fmt"
	"github.com/scrapeless-ai/scrapeless-actor-sdk-go/scrapeless"
	"log"
	"time"
)

type RequestParam struct {
	Name string `json:"name"`
}

func main() {
	actor := scrapeless.New(scrapeless.WithStorage())
	defer actor.Close()
	var param = &RequestParam{}
	if err := actor.Input(param); err != nil {
		panic(err)
	}
	ok, err := actor.Storage.GetDataset().AddItems(context.Background(), []map[string]any{
		{
			"name": param.Name,
		},
	})
	if err != nil {
		panic(err)
	}
	log.Println("Dataset AddItems,", ok)
	for i := 0; i < 10; i++ {
		log.Println("Hello World,", i)
		time.Sleep(time.Second)
	}

	ok, err = actor.Storage.GetKv().SetValue(context.Background(), "key", param.Name, 0)
	if err != nil {
		panic(err)
	}
	log.Println("Dataset AddItems,", ok)
	put, err := actor.Storage.GetObject().Put(context.Background(), "key.json", []byte(fmt.Sprintf(`{"name":"%s"}`, param.Name)))
	if err != nil {
		panic(err)
	}
	log.Println("Dataset AddItems,", put)

}
