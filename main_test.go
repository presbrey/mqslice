package main

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

var (
	testUri string
)

func init() {
	testUri = os.Getenv("AMQP_URI")
	if len(testUri) == 0 {
		testUri = "amqp://localhost/"
	}
}

func binary(ch *amqp.Channel, exchange string, n int) error {
	for i := 0; i < n; i++ {
		next := fmt.Sprintf("%63b", rand.Int63())
		err := ch.Publish(exchange, exchange, false, false, amqp.Publishing{
			Body: []byte(next),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func TestBinary(t *testing.T) {
	s, err := NewServer(testUri, "test.json")
	assert.NoError(t, err)

	ch, err := s.dial()
	assert.NoError(t, err)
	err = ch.ExchangeDeclare("testbin", "fanout", true, false, false, false, nil)
	assert.NoError(t, err)

	err = s.run()
	assert.NoError(t, err)

	zero := 0
	go func() {
		ch, err := s.dial()
		assert.NoError(t, err)
		q, err := ch.QueueDeclare("test-0", false, true, true, false, nil)
		assert.NoError(t, err)
		err = ch.QueueBind(q.Name, "test-0", "test-0", false, nil)
		assert.NoError(t, err)
		consumer, err := ch.Consume(q.Name, "mqslice", true, true, false, false, nil)
		assert.NoError(t, err)
		for elt := range consumer {
			assert.Equal(t, elt.Body[0], uint8(' '))
			zero += 1
		}
	}()

	one := 0
	go func() {
		ch, err := s.dial()
		assert.NoError(t, err)
		q, err := ch.QueueDeclare("test-1", false, true, true, false, nil)
		assert.NoError(t, err)
		err = ch.QueueBind(q.Name, "test-1", "test-1", false, nil)
		assert.NoError(t, err)
		consumer, err := ch.Consume(q.Name, "mqslice", true, true, false, false, nil)
		assert.NoError(t, err)
		for elt := range consumer {
			assert.Equal(t, elt.Body[0], uint8('1'))
			one += 1
		}
	}()

	go func() {
		bin, err := s.dial()
		assert.NoError(t, err)
		err = binary(bin, "testbin", 1000)
		assert.NoError(t, err)
	}()

	time.Sleep(time.Second)
	s.close()
	s.wait()

	assert.True(t, zero > 100)
	assert.True(t, one > 100)
}
