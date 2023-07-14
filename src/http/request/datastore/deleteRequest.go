/*======================
Datastoreのデータ削除を目的とされたリクエストの受け
========================*/
package request

import (
	"net/http"
	"strings"
	"time"

	ENTITY "bwing.app/src/datastore/entity"
	ERR "bwing.app/src/error"
	REQ "bwing.app/src/http/request"
	POSTD "bwing.app/src/http/request/postdata"
	RES "bwing.app/src/http/response"
	LOG "bwing.app/src/log"

	"github.com/pkg/errors"
)

///////////////////////////////////////////////////
/* =========================================== */
//DELETE  Method request
//※Keyを使って指定、削除するサンプルのみ実装している
/* =========================================== */
func DeleteDs(w http.ResponseWriter, r *http.Request, rq REQ.RequestData) {

	/***共通前準備***/
	rq.ParamsBasic.Cdt = time.Now() //処理開始時間
	REQ.NewActionName(r, &rq)       //URLPathからDSのAction名を取得
	REQ.NewKindName(r, &rq)         //URLPathからDSのKind名を取得

	//Instance interface
	var response RES.DeleteResponse

	/***Entityを削除する各処理へのキッカー***/
	//末尾に指定されたKind名がNsKindsで登録されていない場合、[NoDs]が付与されて以下のCaseで除外される
	switch 0 {

	/*=================================================================================
	【Attachment Delete】
	Datastoreへロードしたデータを削除する
	*/
	case
		strings.Index(rq.Urlpath, "/hmt/attachment/delete/"):

		//Jsonデータを取得
		err := POSTD.ParseAttachmentDeleteJsonData(r, &rq)
		if err != nil {
			ERR.ErrorResponse(w, &rq, err, http.StatusBadRequest)
			return
		}
		//Paramsの型をDSに合わせて変換
		REQ.ConvertTypeParams(&rq)
		//リクエストをロギングする
		LOG.ApiRequestLogging(&rq, LOG.INFO)
		//DSのへロードデータを削除する
		response.DeleteLoad2Datastore(w, r, &rq)

		/*
			=================================================================================
			これより以下、サーバー初期構築時につくられたサンプル実装
			=================================================================================
		*/

		/*=======================================================
		DELETEされたFormデータからKeyを指定して、Entityを削除
		*/
		switch 0 {
		case strings.Index(rq.Urlpath, "/delete/"):

			/***共通前準備***/
			//Instance interface
			var response RES.DeleteResponse
			//URLPathからDSのKind名を取得して判定
			//※前提::Pathの末尾がkind名である
			k, b := ENTITY.GetKindName(rq.UrlPathArry)
			//Validation 登録済みのkindかどうかチェック
			if !b {
				rq.Urlpath = k + rq.Urlpath
			}

			switch 0 {
			case strings.Index(rq.Urlpath, "/delete/usingkey/form"):
				/*-----------SAMPLE--------------
				curl -X DELETE localhost:9090/delete/article/delete/usingkey/form \
				-H "Content-Type: multipart/form-data" \
				-F 'IDKey=1'
				-----------SAMPLE--------------*/

				//Formデータを取得
				POSTD.ParseFormData(w, r, &rq)
				//Keyの型をDSに合わせて変換
				REQ.ConvertTypeKeyParameter(&rq)
				//指定されたKeyを使って削除
				t := false
				response.DeleteUsingKey(w, &rq, k, t)

			/*=======================================================
			DELETEされたJSONデータからKeyを指定して、Entityを削除
			Transactionあり
			*/
			case strings.Index(rq.Urlpath, "/delete/usingkey/json/tran"):
				/*-----------CURL SAMPLE--------------
				curl -X DELETE http://localhost:9090/delete/article/delete/usingkey/json \
				-H "Accept: application/json" \
				-H "Content-type: application/json" -d '
				{
				"IDKey" : 2
				}'
				-----------CURL SAMPLE--------------*/

				//Jsonデータを取得
				if err := POSTD.ParseJsonData(r, &rq); err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					ERR.ErrorResponse(w, &rq, err, http.StatusBadRequest)
				}
				//Keyの型をDSに合わせて変換
				REQ.ConvertTypeKeyParameter(&rq)
				//指定されたKeyを使って削除
				t := true
				response.DeleteUsingKey(w, &rq, k, t)

			/*=======================================================
			DELETEされたJSONデータからKeyを指定して、Entityを削除
			Transactionなし
			*/
			case strings.Index(rq.Urlpath, "/delete/usingkey/json"):
				/*-----------CURL SAMPLE--------------
				curl -X DELETE http://localhost:9090/delete/article/delete/usingkey/json \
				-H "Accept: application/json" \
				-H "Content-type: application/json" -d '
				{
				"IDKey" : 2
				}'
				-----------CURL SAMPLE--------------*/

				//Jsonデータを取得
				if err := POSTD.ParseJsonData(r, &rq); err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					ERR.ErrorResponse(w, &rq, err, http.StatusBadRequest)
				}
				//Keyの型をDSに合わせて変換
				REQ.ConvertTypeKeyParameter(&rq)
				//指定されたKeyを使って削除
				t := false
				response.DeleteUsingKey(w, &rq, k, t)
			}
		}

	/*=======================================================
	例外処理
	*/
	default:
		err := errors.New("path is nothing.")
		if err != nil {
			ERR.ErrorResponse(w, &rq, err, http.StatusBadRequest)
		}
	}
}
