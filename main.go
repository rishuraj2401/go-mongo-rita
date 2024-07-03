package main

import (
	"context"
	"fmt"
	kafkarita "go-mongo/kafka"
	cmd "go-mongo/ritaCmd"
	"strconv"

	"log"
	"os"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/yaml.v2"
)


var brokerAddress string
var primaryDB string
type Config struct {
    MongoDB struct {
        URI        string        `yaml:"uri"`
        Duration   time.Duration `yaml:"duration"`
        PrimaryDB  string        `yaml:"primaryDB"`
        BLDbname   string        `yaml:"blDbname"`
    } `yaml:"mongodb"`

    Zeek struct {
        Path       string   `yaml:"path"`
        TotalChunk string  `yaml:"totalchunk"`
		CurrChunk  string  `yaml:"currentchunk"`
		RitaConfig string  `yaml:"ritaconfig"`
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
   primaryDB=cfg.MongoDB.PrimaryDB

	fmt.Println("Connected to MongoDB!")

	ticker := time.NewTicker(cfg.MongoDB.Duration)
	chunk,_ := strconv.Atoi(cfg.Zeek.CurrChunk)
	totChunk,_:=strconv.Atoi(cfg.Zeek.TotalChunk)
	func() {
		for range ticker.C {

	
			if chunk >= totChunk{
				chunk=0
			}
			
            chunkStr:= strconv.Itoa(chunk)
			cmd.RunCommands(cfg.MongoDB.PrimaryDB, cfg.Zeek.RitaConfig, cfg.Zeek.Path, cfg.Zeek.TotalChunk, chunkStr)
			chunk++
			var wg sync.WaitGroup
			wg.Add(11) // We have two goroutines to wait for

			go func() {
				defer wg.Done()
				getData(client, cfg.MongoDB.PrimaryDB, "beacon", "beacon_rita", chunk-1)
			}()

			go func() {
				defer wg.Done()
				getData(client, cfg.MongoDB.PrimaryDB, "useragent", "user_agent_rita", chunk-1)
			}()

			go func() {
				defer wg.Done()
				getData(client, cfg.MongoDB.PrimaryDB, "beaconSNI", "beacon_SNI_rita" ,chunk-1)
			}()

			go func() {
				defer wg.Done()
				getData(client, cfg.MongoDB.PrimaryDB, "explodedDns", "DNS_rita" ,chunk-1)
			}()

			go func() {
				defer wg.Done()
				getData(client, cfg.MongoDB.PrimaryDB, "uconn", "uconn_rita", chunk-1)
			}()

			go func() {
				defer wg.Done()
				getData(client, cfg.MongoDB.PrimaryDB, "SNIconn", "SNIconn_rita",chunk-1)
			}()

			go func() {
				defer wg.Done()
				getData(client, "rita-bl", "ip", "bl_ip_rita", chunk-1)
			}()

			go func() {
				defer wg.Done()
				getData(client, "rita-bl", "lists", "bl_lists_rita", chunk-1)
			}()

            go func() {
				defer wg.Done()
				getData(client, cfg.MongoDB.PrimaryDB, "host", "host_rita", chunk-1)
			}()

            go func() {
				defer wg.Done()
				getData(client, cfg.MongoDB.PrimaryDB, "hostnames", "hostnames_rita", chunk-1)
			}()

            go func(){
                defer wg.Done()
				cmd.ShowLongConnections(cfg.MongoDB.PrimaryDB, cfg.Kafka.BrokerAddress)    
            }()

			wg.Wait()

		}
	}()

	time.Sleep(5 * time.Second)
	ticker.Stop()

}

func getData(client *mongo.Client, db string, colName string, topic string, chunk int) {
	database := client.Database(db)
	collection := database.Collection(colName)
	var cursor *mongo.Cursor
	var err error
	if db == primaryDB{
		fmt.Println(db, colName)
	cursor, err = collection.Find(context.TODO(), bson.M{"cid":chunk})
	if err != nil {
		log.Fatal(err)
	}
	}else{
		cursor, err = collection.Find(context.TODO(), bson.M{})
	if err != nil {
		log.Fatal(err)
	}
	}
	// var c1 mongo.Cursor
	
	defer cursor.Close(context.TODO())
	for cursor.Next(context.TODO()) {
		var elem bson.Raw
		err := cursor.Decode(&elem)
		// if db ==primaryDB{
		//  fmt.Println(cursor)}
        fmt.Println(topic)
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
