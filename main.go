package main

import (
	"encoding/base64"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"

	"github.com/julienschmidt/httprouter"

	"github.com/wilbyang/redirect-service/dto"
	kafka "github.com/wilbyang/redirect-service/kafka"
	"google.golang.org/protobuf/proto"
)

var logger *slog.Logger
var ch chan *dto.Url

const (
	broker = "localhost:9092"
	topic  = "demo-events2"
)

func init() {
	logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ch = make(chan *dto.Url)
	err := kafka.CreateTopic(broker, topic)
	if err != nil {
		log.Fatal(err)
	}
}

func Index(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	fmt.Fprint(w, "Welcome!\n")
}

func Redirect(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	info := ps.ByName("info")
	url := &dto.Url{}
	decoded, _ := base64.StdEncoding.DecodeString(info)
	proto.Unmarshal(decoded, url)
	logger.Info("Redirecting to ", url.DestUrl, url.ProfileId)
	ch <- url
	http.Redirect(w, r, url.DestUrl, http.StatusSeeOther)

}
func main() {

	link := &dto.Url{}
	link.DestUrl = "http://www.google.com"
	link.ProfileId = "p1"
	link.Compaign = "c1"
	link.UrlId = "u1"

	linkBytes, err := proto.Marshal(link)
	if err != nil {
		log.Fatalln("Failed to encode link:", err)
	}

	linkbytesEncoded := base64.StdEncoding.EncodeToString(linkBytes)
	fmt.Println(linkbytesEncoded)
	router := httprouter.New()
	router.GET("/", Index)
	router.GET("/r/:info", Redirect)
	go kafka.Tokafka(ch, broker, topic)

	log.Fatal(http.ListenAndServe(":8080", router))
}
