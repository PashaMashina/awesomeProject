package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type Candidate struct {
	Id                    int64  `db:"id"`
	CreatedAt             string `db:"created_at"`
	UpdatedAt             string `db:"updated_at"`
	Fio                   string
	About                 string
	Email                 string
	Numz                  string
	Phone                 string
	Course                int32
	ApiToken              sql.NullString `db:"api_token"`
	TrainingGroup         string         `db:"training_group"`
	GroupId               sql.NullInt64  `db:"group_id"`
	MiraId                sql.NullString `db:"mira_id"`
	CanSendParticipations int32          `db:"can_send_participations"`
}

type RecordSet struct {
	AICCandidates []*AICCandidate `json:"RecordSet"`
}

type AICCandidate struct {
	Miraid  string `json:"miraid"`
	Nomz    string `json:"nomz"`
	Name    string `json:"name"`
	Email   string `json:"email"`
	Grup    string `json:"grup"`
	Kurs    string `json:"kurs"`
	Kafid   string `json:"kafid"`
	Kafname string `json:"kafname"`
	Facid   string `json:"facid"`
	Facname string `json:"facname"`
}

func main() {
	db, err := Connect("root:228322@tcp(localhost:3306)/YP")
	if err != nil {
		log.Fatal(err)
	}

	var candidates []*Candidate
	err = db.Select(&candidates, "select * from candidates")
	if err != nil {
		log.Fatal(err)
	}

	get, err := SendGet("https://int.istu.edu/extranet/worker/rp_view/integration/03ae8597-3487-46dc-839e-3317af096889/98tR6K7Iz5T8/yarmproj.stud")
	if err != nil {
		log.Fatal(err)
	}

	var set *RecordSet
	err = json.Unmarshal(get, &set)
	if err != nil {
		log.Fatal(err)
	}

	newCand := make([]*Candidate, 0, 0)

	flag := false
	for _, aic := range set.AICCandidates {
		for _, c := range candidates {
			if aic.Nomz == c.Numz {
				flag = true
				break
			}
		}

		if !flag {
			i, err := strconv.ParseInt(aic.Kurs, 10, 64)
			if err != nil {
				log.Fatal(err)
			}

			newCand = append(newCand, &Candidate{
				CreatedAt: time.Now().Format("2006-01-02 15:04:05"),
				UpdatedAt: time.Now().Format("2006-01-02 15:04:05"),
				Fio:       aic.Name,
				Email:     aic.Email,
				Numz:      aic.Nomz,
				MiraId: sql.NullString{
					String: aic.Miraid,
					Valid:  true,
				},
				Course:                int32(i),
				TrainingGroup:         aic.Grup,
				CanSendParticipations: 1,
			})
		}

		flag = false
	}

	flag = false
	updateNotExistCand := make([]*Candidate, 0, 0)
	updateExistCand := make([]*Candidate, 0, 0)
	for _, c := range candidates {
		for _, aic := range set.AICCandidates {
			if c.Numz == aic.Nomz {
				flag = true

				i, err := strconv.ParseInt(aic.Kurs, 10, 64)
				if err != nil {
					log.Fatal(err)
				}
				updateExistCand = append(updateExistCand, &Candidate{
					UpdatedAt: time.Now().Format("2006-01-02 15:04:05"),
					Numz:      aic.Nomz,
					Course:    int32(i),
					MiraId: sql.NullString{
						String: aic.Miraid,
						Valid:  true,
					},
					TrainingGroup:         aic.Grup,
					CanSendParticipations: 1,
				})

				break
			}
		}

		if !flag {
			updateNotExistCand = append(updateNotExistCand, &Candidate{
				Fio:                   c.Fio,
				Numz:                  c.Numz,
				UpdatedAt:             time.Now().Format("2006-01-02 15:04:05"),
				CanSendParticipations: 0,
			})
		}
		flag = false
	}

	log.Println(len(newCand))
	log.Println(len(updateExistCand))
	log.Println(len(updateNotExistCand))

	insert := ""
	for _, candidate := range newCand {
		if candidate.Course == 2 {
			candidate.CanSendParticipations = 0
		}

		insert += fmt.Sprintf(`
insert into candidates(
	created_at,
	updated_at,
	mira_id,
	phone,
	about,
	fio,
	email,
	numz,
	course,
	training_group,
	can_send_participations
) values (
	'%s',
	'%s',
	'%s',
	'',
	'',
	'%s',
	'%s',
	'%s',
	'%d',
	'%s',
	'%d'
);`,
			candidate.CreatedAt,
			candidate.UpdatedAt,
			candidate.MiraId.String,
			candidate.Fio,
			candidate.Email,
			candidate.Numz,
			candidate.Course,
			candidate.TrainingGroup,
			candidate.CanSendParticipations,
		) + "\n"
	}

	update := ""
	for _, candidate := range updateExistCand {
		update += fmt.Sprintf(`
update candidates set 
	mira_id = '%s' and 
	updated_at = '%s' and 
	course = '%d' and 
	training_group = '%s' and 
	can_send_participations = '%d' 
where numz = '%s';`,
			candidate.MiraId.String,
			candidate.UpdatedAt,
			candidate.Course,
			candidate.TrainingGroup,
			candidate.CanSendParticipations,
			candidate.Numz,
		)
	}

	updateNotExists := ""
	for _, candidate := range updateNotExistCand {
		updateNotExists += fmt.Sprintf(`
update candidates set 
	updated_at = '%s' and 
	can_send_participations = '%d' 
where numz = '%s';`,
			candidate.UpdatedAt,
			candidate.CanSendParticipations,
			candidate.Numz,
		)
	}

	err = ioutil.WriteFile("update.sql", []byte(insert+"\n"+update+"\n"+updateNotExists), 0644)
	if err != nil {
		log.Fatal(err)
	}
}

func SendGet(uri string) ([]byte, error) {
	resp, err := http.Get(uri)
	if err != nil {
		log.Fatalln(err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}

	return body, nil
}

func Connect(uri string) (*sqlx.DB, error) {
	db, err := sqlx.Connect(
		"mysql",
		uri,
	)
	if err != nil {
		return nil, err
	}

	return db, nil
}
