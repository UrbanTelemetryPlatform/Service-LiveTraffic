package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"

	"google.golang.org/appengine"

	"encoding/json"

	_ "github.com/lib/pq"
)

type output struct {
	success bool
	error   string
	data    interface{}
}

type livedata struct {
	SEGMENTID int64
	TIME      string
	SPEED     int64
}

var db *sql.DB

func main() {

	datastoreName := os.Getenv("POSTGRES_CONNECTION")

	var err error
	db, err = sql.Open("postgres", datastoreName)
	if err != nil {
		log.Fatal(err.Error())
	}

	// Ensure the table exists. Running an SQL query also checks the connection to the PostgreSQL server
	if err := createTable(); err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/api/update", updateLivedata)
	http.HandleFunc("/api/read", readLiveData)
	http.HandleFunc("/api/welcome", sayHello)
	appengine.Main()
}

func createTable() error {
	stmt := `CREATE TABLE IF NOT EXISTS livedata (
					segmentid  INTEGER PRIMARY KEY,
					time       TIMESTAMP with time zone,
					speed	   INTEGER
			)`
	_, err := db.Exec(stmt)
	return err
}

func sayHello(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "This is the APIConnector \n")
}

func readLiveData(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			log.Print("Recovered in f", r)
		}
	}()

	log.Print("Read live data request")

	stmt := `SELECT segmentid,time,speed FROM livedata`
	rows, err := db.Query(stmt)
	if err != nil {
		msg := fmt.Sprintf("Could not execute select: %v", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	tableData := make([]livedata, 0)

	for rows.Next() {
		var entry livedata

		err := rows.Scan(&entry.SEGMENTID, &entry.TIME, &entry.SPEED)
		if err != nil {
			msg := fmt.Sprintf("Could not retrieve columns: %v", err)
			http.Error(w, msg, http.StatusInternalServerError)
			return
		}

		tableData = append(tableData, entry)
	}

	jsonString, err := json.Marshal(tableData)
	if err != nil {
		msg := fmt.Sprintf("Could not convert to JSON: %v", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, string(jsonString))

}

func updateLivedata(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			log.Print("Recovered in f", r)
		}
	}()

	log.Print("Update live data request")

	if r.Method != "POST" {
		fmt.Fprint(w, "Only POST requests allowed")
		w.WriteHeader(403)
	}

	var input livedata
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&input)
	if err != nil {
		msg := fmt.Sprintf("Could not understand JSON: %v", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	stmt := "INSERT INTO livedata (segmentid, time, speed) VALUES ($1,$2,$3) "
	stmt += "ON CONFLICT (segmentid) DO UPDATE SET speed = $3, time = $2 WHERE livedata.segmentid = $1"
	_, err = db.Exec(stmt, input.SEGMENTID, input.TIME, input.SPEED)
	if err != nil {
		msg := fmt.Sprintf("Could not insert data: %v", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

}
