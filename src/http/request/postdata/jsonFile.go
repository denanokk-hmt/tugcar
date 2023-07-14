/*======================
POSTリクエストされたパラメーターの受信
JsonFileを処理
========================*/
package postdata

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	REQ "bwing.app/src/http/request"
)

func ParseJsonFile(w http.ResponseWriter, r *http.Request, rq *REQ.RequestData) error {
	type Person struct {
		Id       int    `json:"id"`
		Name     string `json:"name"`
		Birthday string `json:"birthday"`
	}
	// JSONファイル読み込み
	bytes, err := ioutil.ReadFile("vro.json")
	if err != nil {
		log.Fatal(err)
	}
	// JSONデコード
	var persons []Person
	if err := json.Unmarshal(bytes, &persons); err != nil {
		log.Fatal(err)
	}
	// デコードしたデータを表示
	for _, p := range persons {
		fmt.Printf("%d : %s\n", p.Id, p.Name)
	}

	return nil
}
