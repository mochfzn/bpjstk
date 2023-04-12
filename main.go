package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

type Response struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

func Connect() *sql.DB {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", "localhost", 5432, "postgres", "postgres", "postgres")
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}

	return db
}

func homePage(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Welcome to the HomePage!")
	fmt.Println("Endpoint Hit: homePage")
}

func create(w http.ResponseWriter, r *http.Request) {
	var (
		response Response
		request  Request
		wg       sync.WaitGroup
	)

	db := Connect()
	defer db.Close()

	reqBody, _ := io.ReadAll(r.Body)
	json.Unmarshal(reqBody, &request)

	limit := 50
	total := len(request.Data)
	division := total / limit
	module := total % limit

	produce := make(chan int, 20)

	wg.Add(10)

	for gr := 1; gr <= 10; gr++ {
		go func(tasks chan int) {
			defer wg.Done()

			for {
				task, ok := <-tasks
				if !ok {
					return
				}

				start := task
				end := task + limit
				penjualan := request.Data[start:end]

				// for _, value := range request.Data[start:end] {
				// 	_, err := db.Exec("INSERT INTO sales (id, customer, quantity, price, timestamp) VALUES (?,?,?,?,?)", value.Id, value.Customer, value.Quantity, value.Price, value.Timestamp)
				// 	if err != nil {
				// 		log.Print(err)
				// 	}
				// }

				_, err := db.Exec(`INSERT INTO sales (id, customer, quantity, price, timestamp) 
					VALUES 
					($1,$2,$3,$4,$5),
					($6,$7,$8,$9,$10), 
					($11,$12,$13,$14,$15), 
					($16,$17,$18,$19,$20), 
					($21,$22,$23,$24,$25), 
					($26,$27,$28,$29,$30), 
					($31,$32,$33,$34,$35), 
					($36,$37,$38,$39,$40), 
					($41,$42,$43,$44,$45), 
					($46,$47,$48,$49,$50), 
					($51,$52,$53,$54,$55), 
					($56,$57,$58,$59,$60), 
					($61,$62,$63,$64,$65), 
					($66,$67,$68,$69,$70), 
					($71,$72,$73,$74,$75), 
					($76,$77,$78,$79,$80), 
					($81,$82,$83,$84,$85), 
					($86,$87,$88,$89,$90), 
					($91,$92,$93,$94,$95), 
					($96,$97,$98,$99,$100),
					($101,$102,$103,$104,$105),
					($106,$107,$108,$109,$110),
					($111,$112,$113,$114,$115),
					($116,$117,$118,$119,$120),
					($121,$122,$123,$124,$125),
					($126,$127,$128,$129,$130),
					($131,$132,$133,$134,$135),
					($136,$137,$138,$139,$140),
					($141,$142,$143,$144,$145),
					($146,$147,$148,$149,$150),
					($151,$152,$153,$154,$155),
					($156,$157,$158,$159,$160), 
					($161,$162,$163,$164,$165), 
					($166,$167,$168,$169,$170), 
					($171,$172,$173,$174,$175), 
					($176,$177,$178,$179,$180), 
					($181,$182,$183,$184,$185), 
					($186,$187,$188,$189,$190), 
					($191,$192,$193,$194,$195), 
					($196,$197,$198,$199,$200),
					($201,$202,$203,$204,$205),
					($206,$207,$208,$209,$210), 
					($211,$212,$213,$214,$215), 
					($216,$217,$218,$219,$220), 
					($221,$222,$223,$224,$225), 
					($226,$227,$228,$229,$230), 
					($231,$232,$233,$234,$235), 
					($236,$237,$238,$239,$240), 
					($241,$242,$243,$244,$245), 
					($246,$247,$248,$249,$250)`,
					penjualan[0].Id, penjualan[0].Customer, penjualan[0].Quantity, penjualan[0].Price, penjualan[0].Timestamp,
					penjualan[1].Id, penjualan[1].Customer, penjualan[1].Quantity, penjualan[1].Price, penjualan[1].Timestamp,
					penjualan[2].Id, penjualan[2].Customer, penjualan[2].Quantity, penjualan[2].Price, penjualan[2].Timestamp,
					penjualan[3].Id, penjualan[3].Customer, penjualan[3].Quantity, penjualan[3].Price, penjualan[3].Timestamp,
					penjualan[4].Id, penjualan[4].Customer, penjualan[4].Quantity, penjualan[4].Price, penjualan[4].Timestamp,
					penjualan[5].Id, penjualan[5].Customer, penjualan[5].Quantity, penjualan[5].Price, penjualan[5].Timestamp,
					penjualan[6].Id, penjualan[6].Customer, penjualan[6].Quantity, penjualan[6].Price, penjualan[6].Timestamp,
					penjualan[7].Id, penjualan[7].Customer, penjualan[7].Quantity, penjualan[7].Price, penjualan[7].Timestamp,
					penjualan[8].Id, penjualan[8].Customer, penjualan[8].Quantity, penjualan[8].Price, penjualan[8].Timestamp,
					penjualan[9].Id, penjualan[9].Customer, penjualan[9].Quantity, penjualan[9].Price, penjualan[9].Timestamp,
					penjualan[10].Id, penjualan[10].Customer, penjualan[10].Quantity, penjualan[10].Price, penjualan[10].Timestamp,
					penjualan[11].Id, penjualan[11].Customer, penjualan[11].Quantity, penjualan[11].Price, penjualan[11].Timestamp,
					penjualan[12].Id, penjualan[12].Customer, penjualan[12].Quantity, penjualan[12].Price, penjualan[12].Timestamp,
					penjualan[13].Id, penjualan[13].Customer, penjualan[13].Quantity, penjualan[13].Price, penjualan[13].Timestamp,
					penjualan[14].Id, penjualan[14].Customer, penjualan[14].Quantity, penjualan[14].Price, penjualan[14].Timestamp,
					penjualan[15].Id, penjualan[15].Customer, penjualan[15].Quantity, penjualan[15].Price, penjualan[15].Timestamp,
					penjualan[16].Id, penjualan[16].Customer, penjualan[16].Quantity, penjualan[16].Price, penjualan[16].Timestamp,
					penjualan[17].Id, penjualan[17].Customer, penjualan[17].Quantity, penjualan[17].Price, penjualan[17].Timestamp,
					penjualan[18].Id, penjualan[18].Customer, penjualan[18].Quantity, penjualan[18].Price, penjualan[18].Timestamp,
					penjualan[19].Id, penjualan[19].Customer, penjualan[19].Quantity, penjualan[19].Price, penjualan[19].Timestamp,
					penjualan[20].Id, penjualan[20].Customer, penjualan[20].Quantity, penjualan[20].Price, penjualan[20].Timestamp,
					penjualan[21].Id, penjualan[21].Customer, penjualan[21].Quantity, penjualan[21].Price, penjualan[21].Timestamp,
					penjualan[22].Id, penjualan[22].Customer, penjualan[22].Quantity, penjualan[22].Price, penjualan[22].Timestamp,
					penjualan[23].Id, penjualan[23].Customer, penjualan[23].Quantity, penjualan[23].Price, penjualan[23].Timestamp,
					penjualan[24].Id, penjualan[24].Customer, penjualan[24].Quantity, penjualan[24].Price, penjualan[24].Timestamp,
					penjualan[25].Id, penjualan[25].Customer, penjualan[25].Quantity, penjualan[25].Price, penjualan[25].Timestamp,
					penjualan[26].Id, penjualan[26].Customer, penjualan[26].Quantity, penjualan[26].Price, penjualan[26].Timestamp,
					penjualan[27].Id, penjualan[27].Customer, penjualan[27].Quantity, penjualan[27].Price, penjualan[27].Timestamp,
					penjualan[28].Id, penjualan[28].Customer, penjualan[28].Quantity, penjualan[28].Price, penjualan[28].Timestamp,
					penjualan[29].Id, penjualan[29].Customer, penjualan[29].Quantity, penjualan[29].Price, penjualan[29].Timestamp,
					penjualan[30].Id, penjualan[30].Customer, penjualan[30].Quantity, penjualan[30].Price, penjualan[30].Timestamp,
					penjualan[31].Id, penjualan[31].Customer, penjualan[31].Quantity, penjualan[31].Price, penjualan[31].Timestamp,
					penjualan[32].Id, penjualan[32].Customer, penjualan[32].Quantity, penjualan[32].Price, penjualan[32].Timestamp,
					penjualan[33].Id, penjualan[33].Customer, penjualan[33].Quantity, penjualan[33].Price, penjualan[33].Timestamp,
					penjualan[34].Id, penjualan[34].Customer, penjualan[34].Quantity, penjualan[34].Price, penjualan[34].Timestamp,
					penjualan[35].Id, penjualan[35].Customer, penjualan[35].Quantity, penjualan[35].Price, penjualan[35].Timestamp,
					penjualan[36].Id, penjualan[36].Customer, penjualan[36].Quantity, penjualan[36].Price, penjualan[36].Timestamp,
					penjualan[37].Id, penjualan[37].Customer, penjualan[37].Quantity, penjualan[37].Price, penjualan[37].Timestamp,
					penjualan[38].Id, penjualan[38].Customer, penjualan[38].Quantity, penjualan[38].Price, penjualan[38].Timestamp,
					penjualan[39].Id, penjualan[39].Customer, penjualan[39].Quantity, penjualan[39].Price, penjualan[39].Timestamp,
					penjualan[40].Id, penjualan[40].Customer, penjualan[40].Quantity, penjualan[40].Price, penjualan[40].Timestamp,
					penjualan[41].Id, penjualan[41].Customer, penjualan[41].Quantity, penjualan[41].Price, penjualan[41].Timestamp,
					penjualan[42].Id, penjualan[42].Customer, penjualan[42].Quantity, penjualan[42].Price, penjualan[42].Timestamp,
					penjualan[43].Id, penjualan[43].Customer, penjualan[43].Quantity, penjualan[43].Price, penjualan[43].Timestamp,
					penjualan[44].Id, penjualan[44].Customer, penjualan[44].Quantity, penjualan[44].Price, penjualan[44].Timestamp,
					penjualan[45].Id, penjualan[45].Customer, penjualan[45].Quantity, penjualan[45].Price, penjualan[45].Timestamp,
					penjualan[46].Id, penjualan[46].Customer, penjualan[46].Quantity, penjualan[46].Price, penjualan[46].Timestamp,
					penjualan[47].Id, penjualan[47].Customer, penjualan[47].Quantity, penjualan[47].Price, penjualan[47].Timestamp,
					penjualan[48].Id, penjualan[48].Customer, penjualan[48].Quantity, penjualan[48].Price, penjualan[48].Timestamp,
					penjualan[49].Id, penjualan[49].Customer, penjualan[49].Quantity, penjualan[49].Price, penjualan[49].Timestamp,
				)
				if err != nil {
					log.Print(err)
				}
			}

		}(produce)
	}

	for post := 0; post < division*limit; post += limit {
		produce <- post
	}

	if module != 0 {
		produce <- division * limit
	}

	close(produce)

	wg.Wait()

	response.Status = "Berhasil"
	response.Message = "Seluruh data berhasil disimpan"

	fmt.Println("Endpoint Hit: create purchase")
	json.NewEncoder(w).Encode(response)
}

func handleRequest() {
	myRouter := mux.NewRouter().StrictSlash(true)
	myRouter.HandleFunc("/", homePage)
	myRouter.HandleFunc("/sales", create).Methods("POST")
	log.Fatal(http.ListenAndServe(":1234", myRouter))
}

func main() {
	// customers := Customers{}
	// err := customers.Generate()
	// if err != nil {
	// 	fmt.Println(err)
	// }

	// request := Request{}
	// err := request.Generate()
	// if err != nil {
	// 	fmt.Println(err)
	// }
	handleRequest()
}
