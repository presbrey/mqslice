package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"runtime"
	"time"

	"github.com/streadway/amqp"
)

var (
	defaultUri = "amqp://guest:guest@localhost:5672/"

	backoff = flag.Duration("backoff", time.Second, "")
	buffer  = flag.Int("buffer", 2048, "channel capacity + prefetch")
	config  = flag.String("config", "mqslice.json", "config file")
	debug   = flag.Bool("debug", false, "")
	queue   = flag.String("queue", "mqslice", "name of primary intake queue")
	uri     = flag.String("uri", defaultUri, "AMQP URI")
)

func init() {
	flag.Parse()
	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}
}

type Sink struct {
	Regex string
	Type  string

	amqp  *amqp.Channel
	ch    chan []byte
	regex *regexp.Regexp
}

type Server struct {
	Source string
	Sinks  map[string]*Sink
	Uri    string

	ch chan []byte

	alive bool
	kill  chan bool
}

func NewServer(uri, cfg string) (*Server, error) {
	s := new(Server)
	s.alive = true
	s.kill = make(chan bool)
	s.ch = make(chan []byte, *buffer)

	if b, err := ioutil.ReadFile(cfg); err != nil {
		return nil, err
	} else if err := json.Unmarshal(b, s); err != nil {
		return nil, err
	}

	if len(uri) > 0 && uri != defaultUri {
		s.Uri = uri
	}
	for _, v := range s.Sinks {
		v.ch = make(chan []byte, *buffer)
		v.regex = regexp.MustCompile(v.Regex)
	}
	go func() {
		for s.alive {
			select {
			case elt := <-s.ch:
				for _, v := range s.Sinks {
					v.ch <- elt
				}
			}
		}
	}()
	return s, nil
}

func (s *Server) dial() (*amqp.Channel, error) {
	conn, err := amqp.Dial(s.Uri)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	if *buffer < 1 {
		return ch, nil
	}
	err = ch.Qos(*buffer, 0, false)
	if err != nil {
		return nil, err
	}
	return ch, nil
}

func (s *Server) run() error {
	ch, err := s.dial()
	if err != nil {
		return err
	}
	for name, sink := range s.Sinks {
		if err := ch.ExchangeDeclare(name, sink.Type, true, false, false, false, nil); err != nil {
			return err
		}
	}
	for name, sink := range s.Sinks {
		name := name
		sink := sink
		go func() {
			var (
				ch  *amqp.Channel
				elt []byte
				err error
			)
			for s.alive {
				if err != nil {
					log.Println(err)
					time.Sleep(*backoff)
					err = nil
					continue
				}
				if len(elt) == 0 {
					select {
					case elt = <-sink.ch:
					}
				}
				if sink.regex != nil && !sink.regex.Match(elt) {
					elt = nil
					continue
				}
				if ch == nil {
					if ch, err = s.dial(); err != nil {
						continue
					}
				}
				if ch != nil {
					err = ch.Publish(name, name, false, false, amqp.Publishing{Body: elt})
					if err == nil {
						elt = nil
					}
				}
			}
		}()
	}
	go s.consumer()
	return nil
}

func (s *Server) consumer() {
	var (
		ch  *amqp.Channel
		err error
		q   amqp.Queue

		consumer <-chan amqp.Delivery
	)
	for s.alive {
		if err != nil {
			log.Println(err)
			time.Sleep(*backoff)
			err = nil
			continue
		}
		if ch == nil {
			ch, err = s.dial()
		}
		if ch != nil {
			q, err = ch.QueueDeclare(*queue, false, true, true, false, nil)
			if err != nil {
				continue
			}
			err = ch.QueueBind(q.Name, s.Source, s.Source, false, nil)
			if err != nil {
				continue
			}
			consumer, err = ch.Consume(q.Name, "mqslice", true, true, false, false, nil)
			if err != nil {
				continue
			}
			for elt := range consumer {
				s.ch <- elt.Body
			}
			ch.Close()
			ch = nil
		}
	}
}

func (s *Server) close() {
	s.alive = false
	close(s.kill)
	close(s.ch)
}

func (s *Server) wait() {
	select {
	case <-s.kill:
	}
}

func main() {
	forever := make(chan bool, 1)
	forever <- true
	for _ = range forever {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("recover: %+v\n", r)
					time.Sleep(1 * time.Second)
					forever <- true
				}
			}()
			s, err := NewServer(*uri, *config)
			if err != nil {
				log.Fatalln(err)
			}
			if err = s.run(); err == nil {
				s.wait()
			}
			log.Println(err)
			time.Sleep(*backoff)
		}()
	}
}
