package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	wsURL           = "wss://radar-map.ru/ws"
	recentThreshold = 10 * time.Minute
)

var keywords = []string{
	"московская область",
	"белгород",
	"курск",
	"тула",
	"калужская область",
}

var seen = make(map[string]struct{})

type Region struct {
	Name        string `json:"name"`
	Key         string `json:"key"`
	SourceText  string `json:"source_text"`
	LastEventTS int64  `json:"last_event_ts"`
}

type StateMessage struct {
	Type    string            `json:"type"`
	Regions map[string]Region `json:"regions"`
}

type DeltaMessage struct {
	Type  string `json:"type"`
	Patch struct {
		RecentBySource map[string][]Region `json:"recent_by_source"`
	} `json:"patch"`
}

func main() {
	log.SetFlags(0)

	for {
		connect()

		log.Println("Соединение потеряно.")
		log.Println("Повторное подключение через 5 секунд...")

		time.Sleep(5 * time.Second)
	}
}

func connect() {
	header := http.Header{}
	header.Set("Origin", "https://radar-map.ru")
	header.Set("User-Agent", "Mozilla/5.0")

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		log.Println(err)
		return
	}

	defer conn.Close()

	log.Println("Подключено.")

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			log.Println(err)
			return
		}

		handleMessage(data)
	}
}

func handleMessage(data []byte) {
	var meta struct {
		Type string `json:"type"`
	}

	if err := json.Unmarshal(data, &meta); err != nil {
		return
	}

	switch meta.Type {

	case "state":
		handleState(data)

	case "state_delta":
		handleDelta(data)
	}
}

func handleState(data []byte) {
	var state StateMessage

	if err := json.Unmarshal(data, &state); err != nil {
		return
	}

	now := time.Now().Unix()

	for _, region := range state.Regions {

		if region.LastEventTS == 0 {
			continue
		}

		id := makeID(region)

		seen[id] = struct{}{}

		// выводим только события последних 10 минут
		if now-region.LastEventTS <= int64(recentThreshold.Seconds()) {
			printRegion(region)
		}
	}
}

func handleDelta(data []byte) {
	var delta DeltaMessage

	if err := json.Unmarshal(data, &delta); err != nil {
		return
	}

	for _, events := range delta.Patch.RecentBySource {

		for _, region := range events {

			process(region)
		}
	}
}

func process(region Region) {

	if region.LastEventTS == 0 {
		return
	}

	id := makeID(region)

	if _, ok := seen[id]; ok {
		return
	}

	seen[id] = struct{}{}

	printRegion(region)
}

func makeID(region Region) string {
	return fmt.Sprintf("%s_%d", region.Key, region.LastEventTS)
}

func printRegion(region Region) {

	text := strings.ToLower(region.SourceText)

	for _, keyword := range keywords {

		if !strings.Contains(text, strings.ToLower(keyword)) {
			continue
		}

		fmt.Println("============================================================")
		fmt.Println(region.SourceText)
		fmt.Println("Получено:", time.Now().Format("02.01.2006 15:04:05"))
		fmt.Println("============================================================")
		fmt.Println()

		return
	}
}
