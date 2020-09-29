package mq

import (
	"fmt"
	"log"
	"time"
)

// Test入口
// 多个Topic测试
func ManyTopic() {
	m := NewClient()
	defer m.Close()
	m.SetConditions(10)

	top := ""
	for i := 0; i < 10; i++ {
		top = fmt.Sprintf("Golang_%02d", i)
		go sub(m, top)
	}
	manyPub(m)
}

func manyPub(c *Client) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			//多个Topic推送不同你的消息
			for i := 0; i < 10; i++ {
				top := fmt.Sprintf("Golang_%02d", i)
				payload := fmt.Sprintf("Hello_%02d", i)
				err := c.Publish(top, payload)
				if err != nil {
					log.Println("pub message failed")
				}
			}
		default:

		}
	}
}

func sub(c *Client, top string) {
	ch, err := c.Subscribe(top)
	if err != nil {
		log.Printf("sub top:%s failed\n", top)
	}

	for {
		val := c.GetPayLoad(ch)
		if val != nil {
			log.Printf("%s message is %s\n", top, val)
		}
	}
}
