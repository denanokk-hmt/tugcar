/*======================
Datastoreのデータ関連処理を目的とされたリクエストのインターフェース
========================*/
package request

import (
	"net/http"

	REQ "bwing.app/src/http/request"
	BQ "bwing.app/src/http/request/bigquery"
)

type request struct {
}

func NewRequests() REQ.Requests {
	return &request{}
}

///////////////////////////////////////////////////
/* =========================================== */
//Interface reciver for datastore
//一枚のファイルだと縦にみづらいので、メソッド別に分離してみた
/* =========================================== */
func (ra *request) GetDsSwitch(w http.ResponseWriter, r *http.Request) {
	GetDs(w, r, REQ.NewRequestData(r))
}
func (ra *request) PostDsWithJsonSwitch(w http.ResponseWriter, r *http.Request) {
	PostDsWithJson(w, r, REQ.NewRequestData(r))
}
func (ra *request) PostDsSwitch(w http.ResponseWriter, r *http.Request) {
	PostDs(w, r, REQ.NewRequestData(r))
}
func (ra *request) PutDsSwitch(w http.ResponseWriter, r *http.Request) {
	PutDs(w, r, REQ.NewRequestData(r))
}
func (ra *request) DeleteDsSwitch(w http.ResponseWriter, r *http.Request) {
	DeleteDs(w, r, REQ.NewRequestData(r))
}

///////////////////////////////////////////////////
/* =========================================== */
//Interface reciver for BigQuery
//一枚のファイルだと縦にみづらいので、メソッド別に分離してみた
/* =========================================== */
func (ra *request) PostBqWithJsonSwitch(w http.ResponseWriter, r *http.Request) {
	BQ.PostBqWithJson(w, r, REQ.NewRequestData(r))
}
