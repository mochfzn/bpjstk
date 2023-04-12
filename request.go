package main

import (
	"encoding/csv"
	"encoding/json"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type Request struct {
	RequestId int     `json:"request_id"`
	Data      []Sales `json:"data"`
}

type Sales struct {
	Id        int       `json:"id"`
	Customer  string    `json:"customer"`
	Quantity  int       `json:"quantity"`
	Price     float64   `json:"price"`
	Timestamp time.Time `json:"timestamp"`
}

func (input Request) Generate() error {
	var (
		request Request
		sales   []Sales
	)

	csvFile, err := os.Open("customers-1000.csv")
	if err != nil {
		return err
	}

	defer csvFile.Close()

	csvLines, err := csv.NewReader(csvFile).ReadAll()
	if err != nil {
		return err
	}

	csvLines = csvLines[1:]

	for _, line := range csvLines {

		index, err := strconv.Atoi(line[0])
		if err != nil {
			return err
		}

		date, err := time.Parse("2006-01-02", line[10])
		if err != nil {
			return err
		}

		object := Sales{
			Id:        index,
			Customer:  line[2],
			Quantity:  rand.Intn(10-1) + 1,
			Price:     float64(rand.Intn(100-10) + 10),
			Timestamp: date,
		}

		sales = append(sales, object)
	}

	request = Request{
		RequestId: 123455,
		Data:      sales,
	}

	jsonBody, err := json.Marshal(request)
	if err != nil {
		return err
	}

	// Open file using READ & WRITE permission.
	file, err := os.OpenFile("request.json", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	// Write text
	_, err = file.Write(jsonBody)
	if err != nil {
		return err
	}

	// Save file changes.
	err = file.Sync()
	if err != nil {
		return err
	}

	return nil
}
