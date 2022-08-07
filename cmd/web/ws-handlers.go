package main

import (
	"fmt"
	"net/http"

	"github.com/gorilla/websocket"
)

type WebSocketConnection struct {
	*websocket.Conn
}

type WebSocketPayload struct {
	Action      string              `json:"action"`
	Message     string              `json:"message"`
	UserName    string              `json:"username"`
	MessageType string              `json:"message_type"`
	Conn        WebSocketConnection `json:"-"`
}

type WebSocketJSONResponse struct {
	Action  string `json:"action"`
	Message string `json:"message"`
	UserID  int    `json:"user_id"`
}

var upgradeConnection = websocket.Upgrader{

	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

var clients = make(map[WebSocketConnection]string)
var wsChan = make(chan WebSocketPayload)

func (app *application) WsEndPoint(w http.ResponseWriter, r *http.Request) {

	ws, err := upgradeConnection.Upgrade(w, r, nil)
	if err != nil {
		app.errorLog.Println(err)
		return
	}

	app.infoLog.Println(fmt.Sprintf("Client connected from %s", r.RemoteAddr))
	var response WebSocketJSONResponse
	response.Message = "Connected to server"

	err = ws.WriteJSON(response)
	if err != nil {
		app.errorLog.Println(err)
		return
	}

	conn := WebSocketConnection{Conn: ws}
	clients[conn] = ""

	go app.ListenForWS(&conn)

}

func (app *application) ListenForWS(conn *WebSocketConnection) {

	defer func() {
		if r := recover(); r != nil {
			app.errorLog.Println("ERROR: ", fmt.Sprintf("%v", r))
		}
	}()

	var payload WebSocketPayload

	for {
		err := conn.ReadJSON(&payload)
		if err != nil {
			//do nothing
			//fortnite
		} else {

			payload.Conn = *conn
			wsChan <- payload
		}
	}
}

func (app *application) ListenToWSChannel() {

	var response WebSocketJSONResponse
	for {
		e := <-wsChan
		switch e.Action {
		case "deleteUser":
			response.Action = "logout"
			response.Message = "Your account has been deleted"
			app.broadcastToAll(response)

		default:
		}

	}
}

func (app *application) broadcastToAll(response WebSocketJSONResponse) {
	for client := range clients {

		err := client.WriteJSON(response)
		if err != nil {
			app.errorLog.Printf("Websocket err on %s: %s", response.Action, err)
			_ = client.Close()
			delete(clients, client)
		}
	}
}
