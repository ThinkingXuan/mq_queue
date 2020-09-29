package main

import "mq_queue/mq"

func main() {
	// 测试
	mq.TestClient()
	// 测试一个Topic
	mq.OneTopic()
	// 测试多个Topic
	mq.ManyTopic()
}
