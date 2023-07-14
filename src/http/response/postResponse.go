/*
======================
POST Methodリクエストに対する処理を行わせ、結果をレスポンスする
========================
*/
package response

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	COMMON "bwing.app/src/common"
	CONFIG "bwing.app/src/config"
	ENTITY "bwing.app/src/datastore/entity"
	ERR "bwing.app/src/error"
	REQ "bwing.app/src/http/request"

	"net/http"

	ARES "bwing.app/src/http/response/datastore/Article"    //Datastore/Article専用
	IXRES "bwing.app/src/http/response/datastore/ItemIndex" //Datastore/ItemIndex専用
	IRES "bwing.app/src/http/response/datastore/Items"      //Datastore/Items専用
	SRES "bwing.app/src/http/response/datastore/Sku"        //Datastore/Sku専用
	TRES "bwing.app/src/http/response/datastore/Tags"       //Datastore/Tags専用

	FBQ "bwing.app/src/bigquery/bq2bq/FetchBq"
	ABQ "bwing.app/src/bigquery/gcs2bq/ApiRequestLogging"
	CBQ "bwing.app/src/bigquery/gcs2bq/ChainedtagsLogging"
	TABLE "bwing.app/src/bigquery/table"
)

// Inerface
type PostResponse struct{}

///////////////////////////////////////////////////
/* =========================================== */
// Attachment用のデータをDatastoreへLoadする
/* =========================================== */
func (res PostResponse) Load2Datastore(w http.ResponseWriter, r *http.Request, rq *REQ.RequestData) {

	var err error

	//Load処理結果数
	var resultsQty int

	//Laad2Ds処理の振り分け(戻り値：Loadした件数 or Error)
	switch rq.ParamsBasic.Kind {

	//ItemsをBigQueryからLoadする
	case ENTITY.KIND_ITEMS:
		var response IRES.ResItems
		resultsQty, err = response.LoadItemsData(w, r, rq)

	//SkuをBigQueryからLoadする
	case ENTITY.KIND_SKU:
		var response SRES.ResSku
		resultsQty, err = response.LoadSkuData(w, r, rq)

	//TagsをBigQueryからLoadする
	case ENTITY.KIND_TAGS:
		var response TRES.ResTags
		resultsQty, err = response.LoadTagsData(w, r, rq)

	//DSにLoad済みのItems、Tagsを使ってWordChainを形成し、ItemIndexへLoadする
	case ENTITY.KIND_ITEMINDEX:
		var response IXRES.ResItemIndex
		resultsQty, err = response.CreateWordChainAndLoadItemIndex(w, r, rq)

	//
	default:
	}

	//Response
	if err == nil {
		//make response body
		responseOutput := fmt.Sprintf("【Load2Ds】[env:%s][kind:%s][client:%s][qty:%d][cdt:%s][revision:%d][prams:%s]",
			CONFIG.GetConfig("Env"),
			rq.ParamsBasic.Kind,
			rq.ParamsBasic.ClientId,
			resultsQty,
			rq.ParamsBasic.Cdt,
			rq.ParamsBasic.Cdt.Unix(),
			rq.ParamsBasic.ParamsStrings)
		//Response
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode((responseOutput))
	} else {
		//Error resonse
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
	}
}

///////////////////////////////////////////////////
/* =========================================== */
// Attachment用のDatastoreデータをUpdateする
/* =========================================== */
func (res PostResponse) Update2Datastore(w http.ResponseWriter, r *http.Request, rq *REQ.RequestData) {

	var err error

	//更新する対象を指定
	kind := rq.ParamsBasic.Kind
	props := rq.ParamsBasic.Seconddary

	fmt.Println(props)

	//Update処理結果数
	var resultsQty int

	//Update処理の振り分け(戻り値：Loadした件数 or Error)
	switch {

	//LatestRevisionの更新
	case props == ENTITY.PROP_LATESTREVISION:

		//有効化するRevision:パラメーターより取得
		var rev int
		for _, p := range rq.PostParameter {
			if p.Name == ENTITY.PROP_REVISION {
				rev = p.IntValue
				fmt.Println(rev)
				break
			}
		}

		filters := make([]REQ.Filter, 4, 4)

		//更新対象フィルター:既存の有効Revisionを無効に更新(LatestRevision=FALSE)
		filters[0] = REQ.Filter{Name: ENTITY.PROP_LATESTREVISION, Ope: "beq", Value: "true"}      //LatestRevisionがTRUE
		filters[1] = REQ.Filter{Name: ENTITY.PROP_REVISION, Ope: "ine", Value: strconv.Itoa(rev)} //指定Revisionと不一致

		//更新対象フィルター:指定したRevisionを有効に更新(LatestRevision=TRUE)
		filters[2] = REQ.Filter{Name: ENTITY.PROP_LATESTREVISION, Ope: "beq", Value: "false"}     //LatestRevisionがFALSE
		filters[3] = REQ.Filter{Name: ENTITY.PROP_REVISION, Ope: "ieq", Value: strconv.Itoa(rev)} //指定Revisionと一致

		switch kind {
		case ENTITY.KIND_ITEMS:
			var response IRES.ResItems
			resultsQty, err = response.UpdateItemsData(w, r, rq, filters)
		case ENTITY.KIND_SKU:
			var response SRES.ResSku
			resultsQty, err = response.UpdateSkuData(w, r, rq, filters)
		case ENTITY.KIND_TAGS:
			var response TRES.ResTags
			resultsQty, err = response.UpdateTagsData(w, r, rq, filters)
		case ENTITY.KIND_ITEMINDEX:
			var response IXRES.ResItemIndex
			resultsQty, err = response.UpdateItemIndexData(w, r, rq, filters)
		}
	default:
	}

	if err == nil {
		//make response body
		responseOutput := fmt.Sprintf("【Update2Ds】[env:%s][kind:%s][client:%s][qty:%d][cdt:%s][revision:%d][prams:%s]",
			CONFIG.GetConfig("Env"),
			rq.ParamsBasic.Kind,
			rq.ParamsBasic.ClientId,
			resultsQty,
			rq.ParamsBasic.Cdt,
			rq.ParamsBasic.Cdt.Unix(),
			rq.ParamsBasic.ParamsStrings)
		//Response
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode((responseOutput))
	} else {
		//Error resonse
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
	}
}

///////////////////////////////////////////////////
/* =========================================== */
// Chained Tags loggingのGCSバケットをBigQueryへLoadする
/* =========================================== */
func (res PostResponse) LoadGcs2BigQuery(w http.ResponseWriter, r *http.Request, rq *REQ.RequestData) {

	cdt := time.Now()

	//bucket prefix name
	var bucket, middlePath, startDate, endDate string
	for _, p := range rq.PostParameter {
		switch p.Name {
		case "bucket_name":
			bucket = p.StringValue
		case "bucket_prefix":
			if p.StringValue != "" {
				bucket = p.StringValue + "_" + bucket
			}
		case "bucket_suffix":
			if p.StringValue != "" {
				bucket = bucket + "_" + p.StringValue
			}
		case "bucket_middle_path":
			middlePath = p.StringValue
		case "start_date":
			startDate = p.StringValue
		case "end_date":
			endDate = p.StringValue
		}
	}

	//取得するログの期間の日数
	dLen := COMMON.DateDiffCalculator(startDate, endDate, "/")

	//日数から取得するすべての日付を配列で取得
	sArr := COMMON.DateAddCalculator(startDate, "/", dLen)

	//レスポンス
	var err error
	var lr interface{}
	var responseOutput string
	var rc, bQty int
	var date string

	//GCSバケットのログデータをBigQueryにロードする
	switch rq.ParamsBasic.LastPath {
	case "chained_tags_logging", "chainedtags_logging":

		//GCSから抽出、BQへロード
		var f CBQ.FetchGcsChainedTagsLogging
		lr, rc, err = f.FetchGcs(rq, bucket, middlePath, sArr)
		if err != nil {
			ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
			return
		}

	case "api_request_logging":

		//GCSから抽出、BQへロード
		var f ABQ.FetchGcsApiRequestLogging
		lr, rc, err = f.FetchGcs(rq, bucket, middlePath, sArr)
		if err != nil {
			ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
			return
		}

	case "checked_image_urls":

		//GCSから抽出、BQへロード
		var f ABQ.FetchGcsApiRequestLogging
		lr, rc, err = f.FetchGcs(rq, bucket, middlePath, sArr)
		if err != nil {
			ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
			return
		}
	}

	//レスポンスを生成
	results := lr.(*[]TABLE.BqLoadResults)
	bQty = len(*results)
	date = sArr[0] + "-" + sArr[len(sArr)-1]
	responseOutput = fmt.Sprintf("【LOADED】[GCS:%s][client:%s][date:%s][bucketObjQty:%d][BqInsertQty:%d]", rq.ParamsBasic.LastPath, rq.ParamsBasic.ClientId, date, bQty, rc)

	//処理時間計測
	fmt.Printf("Finish!! 経過(seconds): %vns\n", time.Since(cdt).Seconds())

	//Response
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode((responseOutput))
}

///////////////////////////////////////////////////
/* =========================================== */
// 画像URLを生成し、画像の有無判定を行い、BigQueryへLoadする
/* =========================================== */
func (res PostResponse) LoadBq2BigQuery(w http.ResponseWriter, r *http.Request, rq *REQ.RequestData) {

	cdt := time.Now()

	//レスポンス
	var err error
	var responseOutput string
	var rc int

	//GCSバケットのログデータをBigQueryにロードする
	switch rq.ParamsBasic.LastPath {
	case "checked_image_urls":

		//BQへロード
		var f FBQ.FetchBq
		rc, err = f.FetchBqLoad2BqImageUrls(rq)
		if err != nil {
			ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
			return
		}
	}

	//レスポンスを生成
	responseOutput = fmt.Sprintf("【LOADED】[Exec:%s][client:%s][BqInsertQty:%d]", rq.ParamsBasic.LastPath, rq.ParamsBasic.ClientId, rc)

	//処理時間計測
	fmt.Printf("Finish!! 経過(seconds): %vns\n", time.Since(cdt).Seconds())

	//Response
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode((responseOutput))

}

/*
	=================================================================================
	これより以下、サーバー初期構築時につくられたサンプル実装
	=================================================================================
*/

///////////////////////////////////////////////////
/* =========================================== */
//kindをキーを使って挿入
/* =========================================== */
func (res PostResponse) PutUsingKey(w http.ResponseWriter, rq *REQ.RequestData, tran bool) {
	//処理の振り分け
	//※データ構造は、Kindごとに異なる=kindごとの処理を呼ぶ→スクリプト言語のように動的(共通)には出来ないし、しない(=静的)
	switch rq.ParamsBasic.Kind {
	case "Article":
		//Instance interface
		var response ARES.ResArticle
		response.PutUsingKey(w, rq, tran)
	default:
	}
}

///////////////////////////////////////////////////
/* =========================================== */
//kindを一括挿入
/* =========================================== */
func (res PostResponse) PutMultiUsingKeyJson(w http.ResponseWriter, r *http.Request, rq *REQ.RequestData) {
	//処理の振り分け
	//※データ構造は、Kindごとに異なる=kindごとの処理を呼ぶ→スクリプト言語のように動的(共通)には出来ないし、しない(=静的)
	switch rq.ParamsBasic.Kind {
	case "Article":
		var response ARES.ResArticle
		response.PutMultiUsingKeyArticle(w, r, rq)
	default:
	}
}
