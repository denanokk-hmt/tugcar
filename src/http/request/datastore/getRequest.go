/*======================
Datastoreのデータ取得を目的とされたリクエストの受け
========================*/
package request

import (
	"net/http"
	"strings"

	ENTITY "bwing.app/src/datastore/entity"
	ERR "bwing.app/src/error"
	REQ "bwing.app/src/http/request"
	response "bwing.app/src/http/response"

	"github.com/pkg/errors"
)

/*
	=================================================================================
	これより以下、サーバー初期構築時につくられたサンプル実装
	=================================================================================
*/

///////////////////////////////////////////////////
/* =========================================== */
//GET Method request
/* =========================================== */
func GetDs(w http.ResponseWriter, r *http.Request, rq REQ.RequestData) {

	/***共通前準備***/

	//Set Get paramter
	REQ.NewParseForm(r, &rq)
	//URLPathからDSのKind名を取得して判定
	//※前提::Pathの末尾がkind名である
	k, b := ENTITY.GetKindName(rq.UrlPathArry)
	//Validation 登録済みのkindかどうかチェック
	if !b {
		rq.Urlpath = k + rq.Urlpath
	}
	//Instance Response interface
	var response response.GetResponse

	/***Entityを取得する各処理へのキッカー***/
	switch 0 {

	/*=======================================================
	フィルターやオーダーを指定して、GetAllを使って、Entityを複数件取得(key情報の返却なし)
	*/
	case strings.Index(rq.Urlpath, "/get/getall/byfilter/"):
		/*-----------REQUEST SAMPLE--------------
		http://localhost:9090/get/getall/byfilter/Article?filter_Number_gt=1&order_Number=asc
		http://localhost:9090/get/getall/byfilter/Article?filter_Number_gt=0&order_Number=desc&limit=5
		-----------REQUEST SAMPLE--------------*/
		response.GetDsEntities(w, &rq)

	/*=======================================================
	フィルターやオーダーを指定して、Runを使って、Entityを複数件取得
	*/
	case strings.Index(rq.Urlpath, "/get/run/byfilter/"):
		/*-----------REQUEST SAMPLE--------------
		http://localhost:9090/get/run/byfilter/Article?filter_Number_gt=1&order_Number=asc
		-----------REQUEST SAMPLE--------------*/
		//※共通化が出来ないパターン
		response.GetRunByFilter(w, &rq)

	/*=======================================================
	その他はエラー
	*/
	default:
		err := errors.New("path is nothing.::" + rq.Urlpath)
		ERR.ErrorResponse(w, &rq, err, http.StatusBadRequest)
	}
}
