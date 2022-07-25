package main

import (
	"context"
	"errors"
	"flag"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type Consumer struct {
	ID     string
	ctx    context.Context
	reader *kafka.Reader
	logger logrus.FieldLogger
}

func newConsumer(ctx context.Context, lgr logrus.FieldLogger, brokers []string, groupId, topic string) *Consumer {

	id := uuid.New()
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:         brokers,
		GroupID:         groupId,
		Topic:           topic,
		MinBytes:        10e2,
		MaxBytes:        10e6,
		MaxWait:         10 * time.Second,
		ReadLagInterval: -1,
	})

	return &Consumer{
		ID:     id.String(),
		ctx:    ctx,
		reader: reader,
		logger: lgr,
	}
}

func (c *Consumer) consume() {

	c.logger.Infof("running consumer: %s", c.ID)

	for {
		select {
		case <-c.ctx.Done():
			if err := c.reader.Close(); err != nil {
				c.logger.Errorf("could not close the reader: %s", err.Error())
			}

			break
		default:
			msg, err := c.reader.ReadMessage(c.ctx)
			if err != nil {
				c.logger.Errorf("error while reading a message: %s", err.Error())
				continue
			}

			c.printMessage(msg)
		}
	}

}

func (c *Consumer) printMessage(msg kafka.Message) {
	c.logger.Infoln("message")
	c.logger.Infof("--- topic: %s", msg.Topic)
	c.logger.Infof("--- key: %s", string(msg.Key))
	c.logger.Infof("--- value: %s", string(msg.Value))
	c.logger.Infof("--- partition: %d", msg.Partition)
	c.logger.Infof("--- offset: %d", msg.Offset)
	c.logger.Infof("--- high water mark: %d", msg.HighWaterMark)
	c.logger.Infof("--- time: %s", msg.Time.String())
	c.logger.Infoln("end message ---")
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

	if err := validateFlags(); err != nil {
		return
	}

	lgr := logrus.New()
	ctx := context.Background()

	brks := strings.Split(*brokers, ",")

	consumer := newConsumer(ctx, lgr, brks, *groupId, *topic)
	consumer.consume()
}
