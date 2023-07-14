/*======================
Datastoreのデータ作成を目的とされたリクエストの受け
※データ取得も利用
========================*/
package request

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	ERR "bwing.app/src/error"
	REQ "bwing.app/src/http/request"
	POSTD "bwing.app/src/http/request/postdata"
	RES "bwing.app/src/http/response"
	LOG "bwing.app/src/log"

	"github.com/pkg/errors"
)

///////////////////////////////////////////////////
/* =========================================== */
//POST  Method request
/* =========================================== */
func PostDsWithJson(w http.ResponseWriter, r *http.Request, rq REQ.RequestData) {

	var err error

	////////////***共通前準備***////////////

	rq.ParamsBasic.Cdt = time.Now() //処理開始時間
	REQ.NewActionName(r, &rq)       //URLPathからDSのAction名を取得
	REQ.NewKindName(r, &rq)         //URLPathからDSのKind名を取得
	REQ.NewSecondaryName(r, &rq)    //URLPathからAction名の次を取得

	//Jsonデータを取得
	switch rq.ParamsBasic.Action {
	case REQ.ACTION_PATH_LOAD, REQ.ACTION_PATH_UPDATE: //Load, Updateの場合
		err = POSTD.ParseAttachmentLoadJsonData(w, r, &rq)
	case REQ.ACTION_PATH_SEARCH, REQ.ACTION_PATH_GET: //Search, Getの場合
		err = POSTD.ParseAttachmentSearchItemsJsonData(r, &rq)
	default:
		err = errors.New("Cannot using action path.")
	}
	if err != nil {
		ERR.ErrorResponse(w, &rq, err, http.StatusBadRequest)
		return
	}

	//Paramsの型をDSに合わせて変換
	REQ.ConvertTypeParams(&rq)

	//リクエストをロギングする
	go LOG.ApiRequestLogging(&rq, LOG.INFO)

	//サーバーToken認証
	//b := REQ.AuthorizationToken(r, &rq)
	//fmt.Println(b)

	////////////***共通前準備***////////////

	//Instance interface
	var response RES.PostResponse

	/***Entityを登録する各処理へのキッカー***/
	//末尾に指定されたKind名がNsKindsで登録されていない場合、[NoDs]が付与されて以下のCaseで除外される
	switch 0 {

	/*=================================================================================
	Attachement LoadAPI
	BigqueryのDataMartから、マスタデータをDatastoreへロードする
	(Attachemntのデータセットに利用するためのマスタ準備)
	*/
	case
		strings.Index(rq.Urlpath, "/hmt/attachment/load/"):

		//BQからデータをDSへロードする
		response.Load2Datastore(w, r, &rq)

	/*=================================================================================
	Attachement UpdateAPI
	ロードされたDatastoreのマスタデータに対して更新を行う
	(/LatestRevision/ →LatestRevisionをtrueにするRevisionをデータを更新するなど)
	*/
	case
		strings.Index(rq.Urlpath, "/hmt/attachment/update/"):

		//BQからデータをDSへロードする
		response.Update2Datastore(w, r, &rq)

	/*=================================================================================
	Attachement SearchAPI
	*/
	case
		strings.Index(rq.Urlpath, "/hmt/attachment/search/"):

		//Keyの型をDSに合わせて変換
		REQ.ConvertTypeKeyParameter(&rq)

		//ChainedTagsをロギングする
		ch := make(chan bool, 1)
		go LOG.AttachmentLogging(&rq, ch)
		select {
		case <-ch:
			//fmt.Println("Check", <-ch)
		}

		//Datastoreからデータを取得
		var q RES.GetResponse
		q.GetDsEntities(w, &rq)

	/*=================================================================================
	Attachement GetAPI
		/attachment/get/Items
		/attachment/get/SpecialTagItems
	*/
	case
		strings.Index(rq.Urlpath, "/hmt/attachment/get/"):

		//Keyの型をDSに合わせて変換
		REQ.ConvertTypeKeyParameter(&rq)

		//ChainedTagsをロギングする
		ch := make(chan bool, 1)
		go LOG.AttachmentLogging(&rq, ch)
		select {
		case <-ch:
			//fmt.Println(<-ch)
		default:
			//Error
		}

		//Datastoreからデータを取得
		var q RES.GetResponse
		q.GetDsEntities(w, &rq)

	/*=================================================================================
	例外処理
	*/
	default:
		err := errors.New("【Error】path is nothing.")
		if err != nil {
			ERR.ErrorResponse(w, &rq, err, http.StatusBadRequest)
		}
	}
}

///////////////////////////////////////////////////
/*
	=================================================================================
	これより以下、サーバー初期構築時につくられたサンプル実装
	=================================================================================
*/
/* =========================================== */
//POST  Method request
/* =========================================== */
func PostDs(w http.ResponseWriter, r *http.Request, rq REQ.RequestData) {

	/***共通前準備***/
	rq.ParamsBasic.Cdt = time.Now() //処理開始時間
	REQ.NewActionName(r, &rq)       //URLPathからDSのAction名を取得
	REQ.NewKindName(r, &rq)         //URLPathからDSのKind名を取得

	//ActionをBasicに登録(Urlの後ろから2番目:/hmt/attachment/[Action]/[kind])
	arr := strings.Split(rq.Urlpath, "/")
	rq.ParamsBasic.Action = arr[len(arr)-2]

	//Instance interface
	var response RES.PostResponse

	/***Entityを登録する各処理へのキッカー***/
	//末尾に指定されたKind名がNsKindsで登録されていない場合、[NoDs]が付与されて以下のCaseで除外される
	switch 0 {

	/*=======================================================
	POSTされたFormデータからKeyを指定して、Entityを挿入
	※IDKey, NameKey指定なし:AutoIDキーが設定される
	※int64でIDKeyを指定:指定したint64のIDキーが設定される
	※stringでNameKeyを指定:指定したstringのNameキーが設定される
	*/
	case strings.Index(rq.Urlpath, "/post/put/usingkey/form/"):
		/*-----------SAMPLE--------------
		curl -X POST localhost:9090/post/put/usingkey/form/Article \
		-H "Content-Type: multipart/form-data" \
		-F 'IDKey=1' \
		-F 'Body=Apple' \
		-F 'Content=りんご' \
		-F 'Number=1' \
		-F 'Title=Article1'
		-----------SAMPLE--------------*/
		//Formデータを取得
		POSTD.ParseFormData(w, r, &rq)
		//Keyの型をDSに合わせて変換
		REQ.ConvertTypeKeyParameter(&rq)
		//Paramsの型をDSに合わせて変換
		REQ.ConvertTypeParams(&rq)
		//指定されたKeyを使ってDSにデータを挿入
		t := false
		response.PutUsingKey(w, &rq, t)

	/*=======================================================
	POSTされた配列で複数Entity指定したJSONデータでPutMultiでEntity挿入
	*/
	case strings.Index(rq.Urlpath, "/post/putmulti/key/json/"):
		/*-----------SAMPLE--------------
		curl -X POST localhost:9090/post/putmulti/key/json/Article \
		-H "Accept: application/json" \
		-H "Content-type: application/json" -d '[{"NameKey":"Article1", "Body":"Apple", "Content":"りんご", "Number":1, "Title":"Article1"},{"NameKey":"Article2", "Body":"Orange", "Content":"みかん", "Number":2, "Title":"Article2"},{"NameKey":"Article3", "Body":"Grape", "Content":"ぶどう", "Number":3 , "Title":"Article3"},{"NameKey":"Article4", "Body":"Banana", "Content":"ばなな", "Number":4, "Title":"Article4"}]'
		-----------SAMPLE--------------*/
		//Article kind
		response.PutMultiUsingKeyJson(w, r, &rq)

	/*=======================================================
	POSTされたJSONデータからKeyを指定して、Entityを挿入
	※IDKey, NameKey指定なし:AutoIDキーが設定される
	※int64でIDKeyを指定:指定したint64のIDキーが設定される
	※stringでNameKeyを指定:指定したstringのNameキーが設定される
	*Transactionあり
	*/
	case strings.Index(rq.Urlpath, "/post/put/usingkey/json/tran/"):
		/*-----------CURL SAMPLE--------------
		curl -X POST http://localhost:9090/post/put/usingkey/json/Article \
		-H "Accept: application/json" \
		-H "Content-type: application/json" -d '
		{
		"client_id" : "ddf",
		"Body" : "Meron",
		"Content" : "メロン",
		"Number" : 2,
		"Title" : "Article2"
		}'
		-----------CURL SAMPLE--------------*/
		//Jsonデータを取得
		if err := POSTD.ParseJsonData(r, &rq); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			ERR.ErrorResponse(w, &rq, err, http.StatusBadRequest)
		}
		//Keyの型をDSに合わせて変換
		REQ.ConvertTypeKeyParameter(&rq)
		//Paramsの型をDSに合わせて変換
		REQ.ConvertTypeParams(&rq)
		//指定されたKeyを使ってDSにデータを挿入
		t := true
		response.PutUsingKey(w, &rq, t)

	/*=======================================================
	POSTされたJSONデータからKeyを指定して、Entityを挿入
	※IDKey, NameKey指定なし:AutoIDキーが設定される
	※int64でIDKeyを指定:指定したint64のIDキーが設定される
	※stringでNameKeyを指定:指定したstringのNameキーが設定される
	*/
	case strings.Index(rq.Urlpath, "/post/put/usingkey/json/"):
		/*-----------CURL SAMPLE--------------
		curl -X POST http://localhost:9090/post/put/usingkey/json/Article \
		-H "Accept: application/json" \
		-H "Content-type: application/json" -d '
		{
		"IDKey" : 2,
		"Body" : "Meron",
		"Content" : "メロン",
		"Number" : 2,
		"Title" : "Article2"
		}'
		-----------CURL SAMPLE--------------*/
		//Jsonデータを取得
		if err := POSTD.ParseJsonData(r, &rq); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			ERR.ErrorResponse(w, &rq, err, http.StatusBadRequest)
		}
		//Keyの型をDSに合わせて変換
		REQ.ConvertTypeKeyParameter(&rq)
		//Paramsの型をDSに合わせて変換
		REQ.ConvertTypeParams(&rq)
		//指定されたKeyを使ってDSにデータを挿入
		t := false
		response.PutUsingKey(w, &rq, t)

	/*=======================================================
	POSTされた画像を/cmd/upload/iamge_file配下に保存
	*/
	case strings.Index(rq.Urlpath, "/post/upload/"):
		/*-----------CURL SAMPLE--------------
		curl -X POST localhost:9090/post/article/upload \
		-H "Content-Type: multipart/form-data" \
		-H 'Content-Type: multipart/form-data; boundary=--SAMPLE--' \
		-F 'image_file=@./IMG_1226.JPG' \
		-F 'hoge=aaa' \
		-F 'foo=bbb'
		-----------CURL SAMPLE--------------*/
		//image_fileで指定された画像ファイルを保存
		sf, err := POSTD.ImageFile(w, r)
		fmt.Println(sf)
		//Response
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if err != nil {
			ERR.ErrorResponse(w, &rq, err, http.StatusBadRequest)
		} else {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode((sf))
		}
		return

	/*=================================================================================
	例外処理
	*/
	default:
		err := errors.New("【Error】path is nothing.")
		fmt.Println(err)
		if err != nil {
			ERR.ErrorResponse(w, &rq, err, http.StatusBadRequest)
		}
	}
}
