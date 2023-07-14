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

func NewRequests() REQ.Requests2 {
	return &request{}
}

///////////////////////////////////////////////////
/* =========================================== */
//Interface reciver for BigQuery
//一枚のファイルだと縦にみづらいので、メソッド別に分離してみた
/* =========================================== */
func (ra *request) PostBqWithJsonSwitch(w http.ResponseWriter, r *http.Request) {
	BQ.PostBqWithJson(w, r, REQ.NewRequestData(r))
}
