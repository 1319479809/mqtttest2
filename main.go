package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Config struct {
	Host     string
	Port     int
	Action   string
	Topic    string
	Username string
	Password string
	Qos      int
	Tls      bool
	CaCert   string
}

var Host = flag.String("host", "127.0.0.1", "server hostname or IP")            //ip
var Port = flag.Int("port", 1883, "server port")                                //port
var Action = flag.String("action", "pubsub", "pub/sub/pubsub action")           //动作
var Protocol = flag.String("protocol", "mqtt", "mqtt/mqtts/ws/wss")             //协议
var Topic = flag.String("topic", "golang-mqtt/test", "publish/subscribe topic") //主题
var Username = flag.String("username", "emqx", "username")                      //用户
var Password = flag.String("password", "public", "password")                    //密码
var Qos = flag.Int("qos", 0, "MQTT QOS")                                        //QoS  最多一次 至少一次  只有一次
var Tls = flag.Bool("tls", false, "Enable TLS/SSL")                             //是否启用TLS
var CaCert = flag.String("cacert", "./broker.emqx.io-ca.crt", "tls cacert")     //TLS证书

func main() {
	flag.Parse()
	config := Config{Host: *Host, Port: *Port, Action: *Action, Topic: *Topic, Username: *Username, Password: *Password, Qos: *Qos, Tls: *Tls, CaCert: *CaCert}
	protocol := *Protocol
	switch protocol {
	case "mqtt":
		MQTTConnection(config)
	case "mqtts":
		MQTTSConnection(config)
	case "ws":
		WSConnection(config)
	case "wss":
		WSSConnection(config)
	default:
		log.Fatalf("Unsupported protocol: %s", protocol)
	}
}

func Pub(client mqtt.Client, topic string) {
	pubClient := client
	i := 1
	for {
		payload := fmt.Sprintf("%d", i)
		pubClient.Publish(topic, 0, false, payload)
		log.Printf("pub [%s] %s\n", topic, payload)
		//i += 1
		i++
		time.Sleep(1 * time.Second)
	}
}

func Sub(client mqtt.Client, topic string) {
	subClient := client
	subClient.Subscribe(topic, 0, func(subClient mqtt.Client, msg mqtt.Message) {
		log.Printf("sub [%s] %s\n", msg.Topic(), string(msg.Payload()))
	})
	for {
		time.Sleep(1 * time.Second)
	}
}

func PubSub(client mqtt.Client, topic string) {
	go Sub(client, topic)
	Pub(client, topic)
}

func connectByMQTT(config Config) mqtt.Client {
	opts := mqtt.NewClientOptions()
	broker := fmt.Sprintf("tcp://%s:%d", config.Host, config.Port)
	opts.AddBroker(broker)
	opts.SetUsername(config.Username)
	opts.SetPassword(config.Password)
	client := mqtt.NewClient(opts)
	token := client.Connect()
	for !token.WaitTimeout(3 * time.Second) {
	}
	if err := token.Error(); err != nil {
		log.Fatal(err)
	}
	return client
}

func connectByMQTTS(config Config) mqtt.Client {
	var tlsConfig tls.Config
	if config.Tls && config.CaCert == "" {
		log.Fatalln("TLS field in config is required")
	}
	certpool := x509.NewCertPool()
	ca, err := ioutil.ReadFile(config.CaCert)
	if err != nil {
		log.Fatalln(err.Error())
	}
	certpool.AppendCertsFromPEM(ca)
	tlsConfig.RootCAs = certpool

	opts := mqtt.NewClientOptions()
	broker := fmt.Sprintf("ssl://%s:%d", config.Host, config.Port)
	println(broker)
	opts.AddBroker(broker)
	opts.SetUsername(config.Username)
	opts.SetPassword(config.Password)
	opts.SetTLSConfig(&tlsConfig)
	client := mqtt.NewClient(opts)
	token := client.Connect()
	for !token.WaitTimeout(3 * time.Second) {
	}
	if err := token.Error(); err != nil {
		log.Fatal(err)
	}
	return client
}

func connectByWS(config Config) mqtt.Client {
	opts := mqtt.NewClientOptions()
	broker := fmt.Sprintf("ws://%s:%d/mqtt", config.Host, config.Port)
	opts.AddBroker(broker)
	opts.SetUsername(config.Username)
	opts.SetPassword(config.Password)
	client := mqtt.NewClient(opts)
	token := client.Connect()
	for !token.WaitTimeout(3 * time.Second) {
	}
	if err := token.Error(); err != nil {
		log.Fatal(err)
	}
	return client
}

func connectByWSS(config Config) mqtt.Client {
	var tlsConfig tls.Config
	if config.Tls && config.CaCert == "" {
		log.Fatalln("TLS field in config is required")
	}
	certpool := x509.NewCertPool()
	ca, err := ioutil.ReadFile(config.CaCert)
	if err != nil {
		log.Fatalln(err.Error())
	}
	certpool.AppendCertsFromPEM(ca)
	tlsConfig.RootCAs = certpool

	opts := mqtt.NewClientOptions()
	broker := fmt.Sprintf("wss://%s:%d/mqtt", config.Host, config.Port)
	opts.AddBroker(broker)
	opts.SetUsername(config.Username)
	opts.SetPassword(config.Password)
	opts.SetTLSConfig(&tlsConfig)
	client := mqtt.NewClient(opts)
	token := client.Connect()
	for !token.WaitTimeout(3 * time.Second) {
	}
	if err := token.Error(); err != nil {
		log.Fatal(err)
	}
	return client
}

func MQTTSConnection(config Config) {
	client := connectByMQTTS(config)
	action := config.Action
	switch action {
	case "pub":
		Pub(client, config.Topic)
	case "sub":
		Sub(client, config.Topic)
	case "pubsub":
		PubSub(client, config.Topic)
	default:
		log.Fatalf("Unsupported action: %s", action)
	}
}

func MQTTConnection(config Config) {
	client := connectByMQTT(config)
	action := config.Action
	switch action {
	case "pub":
		Pub(client, config.Topic)
	case "sub":
		Sub(client, config.Topic)
	case "pubsub":
		PubSub(client, config.Topic)
	default:
		log.Fatalf("Unsupported action: %s", action)
	}
}

func WSConnection(config Config) {
	client := connectByWS(config)
	action := config.Action
	switch action {
	case "pub":
		Pub(client, config.Topic)
	case "sub":
		Sub(client, config.Topic)
	case "pubsub":
		PubSub(client, config.Topic)
	default:
		log.Fatalf("Unsupported action: %s", action)
	}
}

func WSSConnection(config Config) {
	client := connectByWSS(config)
	action := config.Action
	switch action {
	case "pub":
		Pub(client, config.Topic)
	case "sub":
		Sub(client, config.Topic)
	case "pubsub":
		PubSub(client, config.Topic)
	default:
		log.Fatalf("Unsupported action: %s", action)
	}
}
