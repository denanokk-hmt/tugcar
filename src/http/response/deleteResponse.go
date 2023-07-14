/*======================
DELETE Methodリクエストに対する処理を行わせ、結果をレスポンスする
========================*/
package response

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"

	CONFIG "bwing.app/src/config"
	ENTITY "bwing.app/src/datastore/entity"
	QUERY "bwing.app/src/datastore/query"
	ERR "bwing.app/src/error"
	REQ "bwing.app/src/http/request"
	"cloud.google.com/go/datastore"
	"github.com/pkg/errors"
)

//Inerface
type DeleteResponse struct{}

var q QUERY.Queries

///////////////////////////////////////////////////
/* =========================================== */
// Attachment用のLoadしたデータを
//Revisionを指定して削除する or LatestRevisionを指定して削除する
/* =========================================== */
func (del *DeleteResponse) DeleteLoad2Datastore(w http.ResponseWriter, r *http.Request, rq *REQ.RequestData) {

	var err error

	//Filterを設定する箱を準備（連想配列名にフィルター名、要素に値）
	args := make(map[string]string)
	var revFilterOpe string
	for _, v := range rq.PostParameter {
		if v.Name == "Ope" && v.StringValue != "" {
			revFilterOpe = v.StringValue
		}
	}

	if revFilterOpe == "" {
		revFilterOpe = "ieq"
	}

	//Filterに必要な、Prop名&オペ、値(文字列)を設定
	for _, v := range rq.PostParameter {
		if v.Name == "Revision" && v.IntValue != 0 {
			args["filter_"+v.Name+"_"+revFilterOpe] = strconv.Itoa(v.IntValue)
			break
		}
		if v.Name == "LatestRevision" && v.StringValue != "" {
			args["filter_"+v.Name+"_beq"] = v.StringValue
			break
		}
	}

	//Validation
	if len(args) == 0 {
		err := errors.New("Did not found parameter.")
		ERR.ErrorResponse(w, rq, err, http.StatusBadRequest)
		return
	}

	//Get parameterとしてFilterを設定(Get parameterを初期化)
	REQ.SettingFilterToGetParamter(rq, &args, true)

	//Filterを指定したすべてのEntityを取得する
	_, _, keys, err := q.GetAllByFilterIncludeDsCient(rq)
	if err != nil {
		fmt.Println(err)
		ERR.ErrorResponse(w, rq, err, http.StatusBadRequest)
		return
	}

	//Datastore clientを生成(このClinetを使い回す)
	client, err := datastore.NewClient(context.Background(), CONFIG.GetConfig(CONFIG.PROJECT_ID))
	if err != nil {
		fmt.Printf("Error create datastore client.: %v", err)
		ERR.ErrorResponse(w, rq, err, http.StatusBadRequest)
		return
	}
	defer client.Close()

	//goroutine
	var l sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(keys))

	//1件づつ削除
	for _, key := range keys {
		//Keyの箱を準備
		var ek ENTITY.EntityKey = ENTITY.EntityKey{
			ID: key.ID,
		}
		//l.Lock()
		go DeleteLoad2Datastore(w, rq, ek, key.Parent, client, &wg, &l)
	}
	wg.Wait()

	//Response
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if err != nil {
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(("Finish."))
	}
}

///////////////////////////////////////////////////
//go routine delete load data
func DeleteLoad2Datastore(w http.ResponseWriter, rq *REQ.RequestData, ek ENTITY.EntityKey, pKey *datastore.Key,
	client *datastore.Client, wg *sync.WaitGroup, l *sync.Mutex) {

	defer wg.Done()

	//Delete Entity
	resultNum, err := q.DeleteLoadDataByKey(rq, ek, pKey, client)
	if err != nil {
		fmt.Println(ek, err)
		ERR.ErrorResponse(w, rq, err, http.StatusBadRequest)
		return
	}
	//l.Unlock()
	fmt.Println("【DELETE】", rq.ParamsBasic.Kind, rq.ParamsBasic.ClientId, resultNum, ek, "Success")
}

///////////////////////////////////////////////////
/* =========================================== */
//kindをIDを使って削除する
/* =========================================== */
func (del *DeleteResponse) DeleteUsingKey(w http.ResponseWriter, rq *REQ.RequestData, dsKind string, tran bool) {

	var resultNum int
	var err error

	//Keyの箱を準備
	ek := ENTITY.EntityKey{}

	//Post dataをKeyの箱へ換装する::kindの定義に合わせて
	for n, v := range rq.PostParameter {
		fmt.Println(n, v)
		if v.Name == "IDKey" {
			ek.ID = v.IDKeyValue
		} else if v.Name == "NameKey" {
			ek.Name = v.NameKeyValue
		} else {
			continue
		}
	}

	//Delete Entity
	if !tran {
		resultNum, err = q.DeleteByKey(ek, dsKind)
	} else {
		resultNum, err = q.DeleteByKeyTran(ek, dsKind)
	}

	//Response
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if err != nil {
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode((resultNum))
	}
}
