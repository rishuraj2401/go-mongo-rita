package kafkarita

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

const (
	brokerAddress   = "kafka.cosgrid.com:9092"
)




func PubToKafka(name string, value []byte, kafkaAddress string) {
	// kafkaAddress :=brokerAddress
	// os.Getenv("KAFKA_CONSUMER")
	err:=createTopicIfNotExists(name)
	if err != nil {
		log.Fatalf("Failed to create topic %s: %v", name, err)
	}
	w := &kafka.Writer{
		Addr:  kafka.TCP(kafkaAddress),
		Topic: name,
	}
	// fmt.Println("Writing Data: " + name)
	// fmt.Println(value)
	err = w.WriteMessages(context.Background(),
		kafka.Message{
			Value: value,
		},
	)
	if err != nil {
		// log.Fatal("failed to write messages:", 4455err)
		fmt.Print(err.Error())
	}
	fmt.Println("Writing Data Ended : " + name)
}



func createTopicIfNotExists(topicName string) error {
	conn, err := kafka.Dial("tcp", brokerAddress)
	if err != nil {
		return fmt.Errorf("failed to dial Kafka: %w", err)
	}
	defer conn.Close()

	// Check if topic exists
	partitions, err := conn.ReadPartitions(topicName)
	if err == nil && len(partitions) > 0 {
		// Topic exists
		return nil
	}

	// Topic does not exist, create it
	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("failed to get Kafka controller: %w", err)
	}
	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("failed to dial Kafka controller: %w", err)
	}
	defer controllerConn.Close()

	topicConfig := kafka.TopicConfig{
		Topic:             topicName,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	if err := controllerConn.CreateTopics(topicConfig); err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}
	return nil
}
