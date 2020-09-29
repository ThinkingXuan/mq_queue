package mq

import (
	"log"
	"time"
)

var topic = "topic"

// Test入口
func OneTopic() {
	m := NewClient()
	m.SetConditions(10)
	ch, err := m.Subscribe(topic)

	if err != nil {
		log.Println("subscribe failed")
		return
	}

	go oncePub(m)
	onceSub(ch, m)
	defer m.Close()
}

func oncePub(c *Client) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			err := c.Publish(topic, "Hello")
			if err != nil {
				log.Println("pub message failed")
			}
		default:

		}
	}
}

// 接受订阅消息
func onceSub(m <-chan interface{}, c *Client) {
	for {
		val := c.GetPayLoad(m)
		log.Printf("get message is %s\n", val)
	}
}
