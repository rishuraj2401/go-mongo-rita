package main

import (
	"context"
	"fmt"
	kafkarita "go-mongo/kafka"
	cmd "go-mongo/ritaCmd"
	
	"log"
	"os"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/yaml.v2"
)

// var (
//     uri       = "mongodb://localhost:27017"
// 	duration  = 10* time.Second
// 	primaryDB = "mydatabase"
// 	blDbname  = "rita-bl"	
// 	config = "config.yaml"
// 	zeekPath= "/opt/zeek/logs/current/"
// 	totalchunk = "24"
// 	brokerAddress   = "kafka.cosgrid.com:9092"
	
// )

var brokerAddress string
type Config struct {
    MongoDB struct {
        URI        string        `yaml:"uri"`
        Duration   time.Duration `yaml:"duration"`
        PrimaryDB  string        `yaml:"primaryDB"`
        BLDbname   string        `yaml:"blDbname"`
    } `yaml:"mongodb"`

    Zeek struct {
        Path      string `yaml:"path"`
        TotalChunk string `yaml:"totalchunk"`
		RitaConfig string `yaml:"ritaconfig"`
    } `yaml:"zeek"`

    Kafka struct {
        BrokerAddress string `yaml:"brokerAddress"`
    } `yaml:"kafka"`
}
// var brokerAddress string

func main() {
	var cfg Config

    // Read config file
    data, err := os.ReadFile("mongo.yaml")
    if err != nil {
        log.Fatalf("error reading config file: %v", err)
    }

    // Unmarshal YAML data into config struct
    if err := yaml.Unmarshal(data, &cfg); err != nil {
        log.Fatalf("error unmarshaling config: %v", err)
    }
	clientOptions := options.Client().ApplyURI(cfg.MongoDB.URI)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(context.TODO())

	// Ping the MongoDB server to ensure connection
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}

   brokerAddress= cfg.Kafka.BrokerAddress

	fmt.Println("Connected to MongoDB!")

	ticker := time.NewTicker(cfg.MongoDB.Duration)

	func() {
		for range ticker.C {
			cmd.RunCommands(cfg.MongoDB.PrimaryDB, cfg.Zeek.RitaConfig, cfg.Zeek.Path, cfg.Zeek.TotalChunk)
			var wg sync.WaitGroup
			wg.Add(10) // We have two goroutines to wait for

			go func() {
				defer wg.Done()
				getData(client, cfg.MongoDB.PrimaryDB, "beacon", "beacon_rita")
			}()

			go func() {
				defer wg.Done()
				getData(client, cfg.MongoDB.PrimaryDB, "useragent", "user_agent_rita")
			}()

			go func() {
				defer wg.Done()
				getData(client, cfg.MongoDB.PrimaryDB, "beaconSNI", "beacon_SNI_rita")
			}()

			go func() {
				defer wg.Done()
				getData(client, cfg.MongoDB.PrimaryDB, "explodedDns", "beacon_DNS_rita")
			}()

			go func() {
				defer wg.Done()
				getData(client, cfg.MongoDB.PrimaryDB, "uconn", "uconn_rita")
			}()

			go func() {
				defer wg.Done()
				getData(client, cfg.MongoDB.PrimaryDB, "SNIconn", "SNIconn_rita")
			}()

			go func() {
				defer wg.Done()
				getData(client, "rita-bl", "ip", "bl_ip_rita")
			}()

			go func() {
				defer wg.Done()
				getData(client, "rita-bl", "lists", "bl_lists_rita")
			}()

            go func() {
				defer wg.Done()
				getData(client, cfg.MongoDB.PrimaryDB, "host", "host_rita")
			}()

            go func() {
				defer wg.Done()
				getData(client, cfg.MongoDB.PrimaryDB, "hostnames", "bl_lists_rita")
			}()

            // go func(){
            //     defer wg.Done()
                
            // }

			wg.Wait()

		}
	}()

	time.Sleep(5 * time.Second)
	ticker.Stop()

}

func getData(client *mongo.Client, db string, colName string, topic string) {
	database := client.Database(db)
	collection := database.Collection(colName)
	cursor, err := collection.Find(context.TODO(), bson.D{})
	// var c1 mongo.Cursor
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(context.TODO())
	for cursor.Next(context.TODO()) {
		var elem bson.Raw
		err := cursor.Decode(&elem)
		// fmt.Println(cursor1)

		if err != nil {
			log.Fatal(err)
		}
		jsondata, _ := bson.MarshalExtJSON(elem, false, false)
		kafkarita.PubToKafka(topic, jsondata, brokerAddress)
	}
	if err := cursor.Err(); err != nil {
		log.Fatal(err)
	}
}
