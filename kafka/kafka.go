package kafka

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/wilbyang/redirect-service/dto"
)

const (
	batchBytes = 1024 * 1024
)

func Tokafka(ch <-chan *dto.Url, broker string, topic string) {

	var urls []*dto.Url
	writer := &kafka.Writer{
		Addr:       kafka.TCP(broker),
		Balancer:   &kafka.Hash{},
		BatchBytes: batchBytes,
		Transport:  &kafka.Transport{}, // No TLS or SASL
	}
	ticker := time.NewTicker(time.Second * 2)
	for {
		select {
		case url := <-ch:

			urls = append(urls, url)

		case <-ticker.C:
			if len(urls) > 0 {

				messageBuilder := bytes.Buffer{}
				for _, url := range urls {
					msg := fmt.Sprintf(
						`{"click_ts": "%s", "link_id": "%s", "compaign_id": "%s", "profile_id": "%s"}`,
						time.Now().UTC().Format(time.RFC3339),
						url.UrlId, url.Compaign, url.ProfileId,
					)
					fmt.Println(msg)
					messageBuilder.WriteString(msg)

				}
				messageBuilder.WriteString("\n")
				if err := writer.WriteMessages(context.Background(), kafka.Message{
					Topic:     topic,
					Partition: 0,
					Value:     messageBuilder.Bytes(),
					Time:      time.Now(),
				}); err != nil {
					panic(err)
				}

				urls = nil
			}
		}
	}

}

func CreateTopic(broker string, topic string) error {
	dialer := &kafka.Dialer{} // No TLS or SASL

	brokerConn, err := dialer.DialContext(context.Background(), "tcp", broker)
	if err != nil {
		return err
	}
	defer brokerConn.Close()

	controller, err := brokerConn.Controller()
	if err != nil {
		return err
	}

	controllerConn, err := dialer.DialContext(
		context.Background(),
		"tcp",
		fmt.Sprintf("%s:%d", controller.Host, controller.Port),
	)
	if err != nil {
		return err
	}

	_, err = controllerConn.ReadPartitions(topic)
	if err != kafka.UnknownTopicOrPartition {
		fmt.Printf("topic: %s exists\n", topic)
		return nil
	}
	fmt.Printf("topic: %s does not exist, will create\n", topic)
	if err := controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1, // Default partition count
		ReplicationFactor: 1, // Default replication factor
		ConfigEntries: []kafka.ConfigEntry{
			{ConfigName: "retention.ms", ConfigValue: "21600000"},
			{ConfigName: "retention.bytes", ConfigValue: "5368709120"},
		},
	}); err != nil {
		return err
	}
	fmt.Printf("topic: %s created\n", topic)
	return nil
}
