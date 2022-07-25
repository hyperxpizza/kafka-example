package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type Producer struct {
	ctx    context.Context
	writer *kafka.Writer
	lgr    logrus.FieldLogger
}

func newProducer(ctx context.Context, lgr logrus.FieldLogger, brokers []string, clientID, topic string) *Producer {

	dialer := kafka.Dialer{
		Timeout:  10 * time.Second,
		ClientID: clientID,
	}

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      brokers,
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		Dialer:       &dialer,
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
	})

	return &Producer{
		ctx:    ctx,
		writer: writer,
		lgr:    lgr,
	}
}

func (p *Producer) push(key, value []byte) error {
	msg := kafka.Message{
		Key:   key,
		Value: value,
	}

	if err := p.writer.WriteMessages(p.ctx, msg); err != nil {
		return err
	}

	return nil
}

func (p *Producer) produce() {
	p.lgr.Infoln("starting producer")
	i := 0
	for {
		select {
		case <-p.ctx.Done():
			if err := p.writer.Close(); err != nil {
				p.lgr.Errorf("could not close writer: %s", err.Error())
			}
			break
		default:
			key := fmt.Sprintf("key-%d", i)
			val := fmt.Sprintf("value-%d", i)

			keyB, err := json.Marshal(key)
			if err != nil {
				continue
			}

			valB, err := json.Marshal(val)
			if err != nil {
				continue
			}

			err = p.push(keyB, valB)
			if err != nil {
				p.lgr.Errorf("could not push message: %s", err.Error())
			}

			i++

			time.Sleep(5 * time.Second)
		}

	}
}
func validateFlags() error {
	err := errors.New("invalid flags")

	if brokers == nil || *brokers == "" {
		return err
	}

	if groupId == nil || *groupId == "" {
		return err
	}

	if topic == nil || *topic == "" {
		return err
	}

	return nil
}

var brokers = flag.String("brokers", "", "comma separated list of brokers")
var groupId = flag.String("groupid", "", "group id")
var topic = flag.String("topic", "", "kafka topic")

func main() {

	flag.Parse()

	//sigchan := make(chan os.Signal, 1)
	//signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	lgr := logrus.New()

	if err := validateFlags(); err != nil {
		lgr.Fatal(err)
		return
	}

	ctx := context.Background()
	brks := strings.Split(*brokers, ",")

	producer := newProducer(ctx, lgr, brks, *groupId, *topic)
	producer.produce()
}
