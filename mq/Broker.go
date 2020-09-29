package mq

import (
	"errors"
	"sync"
	"time"
)

type Broker interface {
	// 消息发送
	publish(topic string, msg interface{}) error
	// 消息订阅
	subscribe(topic string) (<-chan interface{}, error)
	// 消息取消订阅
	unsubscribe(topic string, sub <-chan interface{}) error
	// 关闭消息队列
	close()
	// 对推送的消息就行广播
	broadcast(msg interface{}, subscribers []chan interface{})
	//设置消息队列的容量
	sendConditions(capacity int)
}

type BrokerImpl struct {
	// 关闭消息队列
	exit chan bool

	// 消息队列的容量
	capacity int

	// 主题 map结构 key为topic,value为一个chan类型切片
	// 这里这么做的原因是我们一个topic可以有多个订阅者，所以一个订阅者对应着一个通道
	topics map[string][]chan interface{} // key: topic  value: queue

	//读写锁，这里是为了防止并发情况下，数据的推送出现错误，所以采用加锁的方式进行保证
	sync.RWMutex
}

func NewBroker() *BrokerImpl {
	return &BrokerImpl{
		exit:   make(chan bool),
		topics: make(map[string][]chan interface{}),
	}
}

// publish 给指定topic发布消息
func (b *BrokerImpl) publish(topic string, pub interface{}) error {
	select {
	case <-b.exit:
		return errors.New("broker close")
	default:
	}
	b.RLock()
	subscribers, ok := b.topics[topic]
	b.RUnlock()
	if !ok {
		return nil
	}
	b.broadcast(pub, subscribers)
	return nil
}

// subscribe 订阅某个Topic
func (b *BrokerImpl) subscribe(topic string) (<-chan interface{}, error) {

	select {
	case <-b.exit:
		return nil, errors.New("broker closed")
	default:
	}

	ch := make(chan interface{}, b.capacity)
	b.Lock()
	b.topics[topic] = append(b.topics[topic], ch)
	b.Unlock()
	return ch, nil
}

// unsubscribe 取消订阅某个Topic
func (b *BrokerImpl) unsubscribe(topic string, sub <-chan interface{}) error {
	select {
	case <-b.exit:
		return errors.New("broker closed")
	default:
	}

	b.RLock()
	subscribers, ok := b.topics[topic]
	b.RUnlock()

	if !ok {
		return nil
	}

	// delete subscriber
	var newSubs []chan interface{}
	for _, subscriber := range subscribers {
		if subscriber == sub {
			continue
		}
		newSubs = append(newSubs, subscriber)
	}

	b.Lock()
	b.topics[topic] = newSubs
	b.Unlock()
	return nil
}

// close 关闭整个消息队列
func (b *BrokerImpl) close() {
	select {
	case <-b.exit:
		return
	default:
		close(b.exit)
		//这里主要是为了保证下一次使用该消息队列不发生冲突
		b.Lock()
		b.topics = make(map[string][]chan interface{})
		b.Unlock()
	}
	return
}

func (b *BrokerImpl) broadcast(msg interface{}, subscribers []chan interface{}) {
	count := len(subscribers)
	concurrency := 1

	//大量的订阅者时，那推送一次就会耗费很多时间，所以采用这种方法进行分解可以降低一定的时间
	switch {
	case count > 1000:
		concurrency = 3
	case count > 100:
		concurrency = 2
	default:
		concurrency = 1
	}

	//pub := func(start int) {
	//	for j := start; j < count; j += concurrency {
	//		select {
	//		case subscribers[j] <- msg:
	//		case <-time.After(time.Millisecond * 5):
	//		case <-b.exit:
	//			return
	//		}
	//	}
	//}
	//采用Timer 而不是使用time.After 原因：time.After会产生内存泄漏 在计时器触发之前，垃圾回收器不会回收Timer
	idleDuration := 5 * time.Millisecond
	idleTimeout := time.NewTimer(idleDuration)
	defer idleTimeout.Stop()
	pub := func(start int) {
		for j := start; j < count; j += concurrency {
			idleTimeout.Reset(idleDuration)
			select {
			case subscribers[j] <- msg:
			case <-idleTimeout.C:
			case <-b.exit:
				return
			}
		}
	}

	for i := 0; i < concurrency; i++ {
		go pub(i)
	}
}

// setConditions 设置消息队列容量
func (b *BrokerImpl) setConditions(capacity int) {
	b.capacity = capacity
}
