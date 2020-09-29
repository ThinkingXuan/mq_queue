package mq

import (
	"fmt"
	"log"
	"sync"
)

func TestClient() {
	b := NewClient()
	b.SetConditions(100)

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		topic := fmt.Sprintf("Goland_%d", i)
		payload := fmt.Sprintf("Hello_%d", i)

		// 订阅
		ch, err := b.Subscribe(topic)
		if err != nil {
			log.Fatal(err)
		}

		wg.Add(1)
		go func() {
			e := b.GetPayLoad(ch)
			if e != payload {
				log.Fatalf("%s expected %s but get %s", topic, payload, e)
			}
			//取消订阅
			if err := b.Unsubscribe(topic, ch); err != nil {
				log.Fatal(err)
			}
			wg.Done()

		}()

		// 发布
		if err := b.Publish(topic, payload); err != nil {
			log.Fatal(err)
		}
	}
	wg.Wait()
}
