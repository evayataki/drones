package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"

	"github.com/gorilla/websocket"
)

const (
	wsURL           = "wss://radar-map.ru/ws"
	recentThreshold = 10 * time.Minute
	pingInterval    = 30 * time.Second
)

var keywords = []string{
	"московская область",
	"коломна",
}

var (
	seen     = make(map[string]struct{})
	botToken string
	chatID   string
)

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

type TelegramMessage struct {
	ChatID string `json:"chat_id"`
	Text   string `json:"text"`
}

func main() {

	if err := godotenv.Load(); err != nil {
		log.Println(".env не найден, использую переменные окружения")
	}

	log.SetFlags(log.LstdFlags)

	botToken = os.Getenv("BOT_TOKEN")
	chatID = os.Getenv("CHAT_ID")

	if botToken == "" {
		log.Fatal("BOT_TOKEN не задан")
	}

	if chatID == "" {
		log.Fatal("CHAT_ID не задан")
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	startMessage := fmt.Sprintf(
		"Сервис radar запущен.\n\n"+
			"Сервер: %s\n"+
			"Время: %s",
		hostname,
		time.Now().Format("02.01.2006 15:04:05"),
	)

	if err := sendTelegram(startMessage); err != nil {
		log.Println("Не удалось отправить сообщение о запуске:", err)
	} else {
		log.Println("Отправлено сообщение о запуске.")
	}

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

	conn.SetReadDeadline(time.Now().Add(2 * pingInterval))

	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(2 * pingInterval))
		return nil
	})

	go func() {
		ticker := time.NewTicker(pingInterval)
		defer ticker.Stop()

		for range ticker.C {
			err := conn.WriteControl(
				websocket.PingMessage,
				[]byte{},
				time.Now().Add(5*time.Second),
			)
			if err != nil {
				return
			}
		}
	}()

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

		if now-region.LastEventTS <= int64(recentThreshold.Seconds()) {
			notify(region)
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

	notify(region)
}

func makeID(region Region) string {
	return fmt.Sprintf("%s_%d", region.Key, region.LastEventTS)
}

func notify(region Region) {

	text := strings.ToLower(region.SourceText)

	for _, keyword := range keywords {

		if !strings.Contains(text, strings.ToLower(keyword)) {
			continue
		}

		msg := fmt.Sprintf(
			"%s\n\nПолучено: %s",
			region.SourceText,
			time.Now().Format("02.01.2006 15:04:05"),
		)

		if err := sendTelegram(msg); err != nil {
			log.Println(err)
			return
		}

		log.Printf("Отправлено: %s\n", region.Name)

		return
	}
}

func sendTelegram(text string) error {

	req := TelegramMessage{
		ChatID: chatID,
		Text:   text,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return err
	}

	resp, err := http.Post(
		fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", botToken),
		"application/json",
		bytes.NewBuffer(body),
	)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {

		b, _ := io.ReadAll(resp.Body)

		return fmt.Errorf("telegram error: %s", string(b))
	}

	return nil
}
