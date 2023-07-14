/*======================
Datastoreのデータ更新を目的とされたリクエストの受け
========================*/
package request

import (
	"net/http"
	"strings"

	ENTITY "bwing.app/src/datastore/entity"
	ERR "bwing.app/src/error"
	REQ "bwing.app/src/http/request"
	"bwing.app/src/http/request/postdata"
	response "bwing.app/src/http/response/datastore/Article"

	"github.com/pkg/errors"
)

/*
	=================================================================================
	これより以下、サーバー初期構築時につくられたサンプル実装
	=================================================================================
*/

///////////////////////////////////////////////////
/* =========================================== */
//PUT  Method request
/* =========================================== */
func PutDs(w http.ResponseWriter, r *http.Request, rq REQ.RequestData) {

	/***共通前準備***/

	//URLPathからDSのKind名を取得して判定
	//※前提::Pathの末尾がkind名である
	k, b := ENTITY.GetKindName(rq.UrlPathArry)
	//Validation 登録済みのkindかどうかチェック
	if !b {
		rq.Urlpath = k + rq.Urlpath
	}
	//Instance Article interface
	var response response.ResArticle

	/***Entityを更新する各処理へのキッカー***/
	switch 0 {

	/*=======================================================
	PUTされたFormデータからKeyを指定して、Entityを上書き
	※POSTと利用するソースは同じ
	*/
	case strings.Index(rq.Urlpath, "/put/put/usingkey/form/"):
		/*-----------SAMPLE--------------
		curl -X PUT localhost:9090/put/put/usingkey/form/Article \
		-H "Content-Type: multipart/form-data" \
		-F 'IDKey=1' \
		-F 'Title=フルーツ1'
		-----------SAMPLE--------------*/
		//Formデータを取得
		postdata.ParseFormData(w, r, &rq)
		//Keyの型をDSに合わせて変換
		REQ.ConvertTypeKeyParameter(&rq)
		//Paramsの型をDSに合わせて変換
		REQ.ConvertTypeParams(&rq)
		//指定されたKeyを使ってDSにデータを上書き
		t := false
		response.PutUsingKey(w, &rq, t)

	/*=======================================================
	  PUTされたJSONデータからKeyを指定して、Entityを上書き
	  ※POSTと利用するソースは同じ
	  *Transactionあり
	*/
	case strings.Index(rq.Urlpath, "/put/put/usingkey/json/tran/"):
		/*-----------CURL SAMPLE--------------
		curl -X PUT http://localhost:9090/put/put/usingkey/json/tran/Article \
		-H "Accept: application/json" \
		-H "Content-type: application/json" -d '
		{
		"IDKey" : 2,
		"Body" : "MaskMeron",
		"Content" : "マスクメロン",
		}'
		-----------CURL SAMPLE--------------*/
		//Jsonデータを取得
		if err := postdata.ParseJsonData(r, &rq); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			ERR.ErrorResponse(w, &rq, err, http.StatusBadRequest)
		}
		//Keyの型をDSに合わせて変換
		REQ.ConvertTypeKeyParameter(&rq)
		//Paramsの型をDSに合わせて変換
		REQ.ConvertTypeParams(&rq)
		//指定されたKeyを使ってDSにデータを上書き
		t := true
		response.PutUsingKey(w, &rq, t)

	/*=======================================================
	PUTされたJSONデータからKeyを指定して、Entityを上書き
	※POSTと利用するソースは同じ
	*/
	case strings.Index(rq.Urlpath, "/put/put/usingkey/json/"):
		/*-----------CURL SAMPLE--------------
		curl -X PUT http://localhost:9090/put/put/usingkey/json/Article \
		-H "Accept: application/json" \
		-H "Content-type: application/json" -d '
		{
		"IDKey" : 2,
		"Body" : "MaskMeron",
		"Content" : "マスクメロン",
		}'
		-----------CURL SAMPLE--------------*/
		//Jsonデータを取得
		if err := postdata.ParseJsonData(r, &rq); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			ERR.ErrorResponse(w, &rq, err, http.StatusBadRequest)
		}
		//Keyの型をDSに合わせて変換
		REQ.ConvertTypeKeyParameter(&rq)
		//Paramsの型をDSに合わせて変換
		REQ.ConvertTypeParams(&rq)
		//指定されたKeyを使ってDSにデータを上書き
		t := false
		response.PutUsingKey(w, &rq, t)

	/*=======================================================
	//Error handle
	*/
	default:
		err := errors.New("path is nothing.")
		if err != nil {
			ERR.ErrorResponse(w, &rq, err, http.StatusBadRequest)
		}
	}
}
