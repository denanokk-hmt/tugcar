/*
======================
Datastoreの
Namespace: WhatYa-Attachment-[client]-[env]
kind: Sku
に対して、結果を取得しレスポンスをする
========================
*/
package response

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	BQ "bwing.app/src/bigquery/bq2ds"
	CONFIG "bwing.app/src/config"
	ENTITY "bwing.app/src/datastore/entity"    //共通Query
	SQUERY "bwing.app/src/datastore/query/Sku" //Sku専用Query
	ERR "bwing.app/src/error"
	REQ "bwing.app/src/http/request"
	"cloud.google.com/go/datastore"
)

// Inerface
type ResSku struct{}

var qs SQUERY.QuerySku

///////////////////////////////////////////////////
/* =========================================== */
//Sku データをBigQueryからロード
//SQLの拡張性を考慮して、マスタ別にこのResponseを準備する
/* =========================================== */
func (res *ResSku) LoadSkuData(w http.ResponseWriter, r *http.Request, rq *REQ.RequestData) (int, error) {

	cdt := time.Now()

	//BigQueryのDataMartから取得したデータを、Datastoreにロードする
	resultsQty, err := BQ.FetchBigQueryLoad2Ds(rq)
	if err != nil {
		return 0, err
	}

	//処理時間計測
	fmt.Printf("Finish!! 経過(seconds): %vns\n", time.Since(cdt).Seconds())

	return resultsQty, nil
}

///////////////////////////////////////////////////
/* =========================================== */
//Sku ロードデータを更新
//更新処理の拡張性を考慮して、マスタ別にこのResponseを準備する
/* =========================================== */
func (res ResSku) UpdateSkuData(w http.ResponseWriter, r *http.Request, rq *REQ.RequestData, filters []REQ.Filter) (int, error) {

	cdt := time.Now()

	var err error

	//Datastore clientを生成(このClinetを使い回す)
	client, err := datastore.NewClient(context.Background(), CONFIG.GetConfig(CONFIG.PROJECT_ID))
	if err != nil {
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
		return 0, err
	}

	//Entityを更新:既存の有効Revisionを無効に更新(LatestRevision=FALSE)
	resultsQty, err := qs.UpdateLatestRevisionByRevision(rq, client, filters[:2], false)
	if err != nil {
		return 0, err
	}

	//Entityを更新:指定したRevisionを有効に更新(LatestRevision=TRUE)
	resultsQty, err = qs.UpdateLatestRevisionByRevision(rq, client, filters[2:], true)
	if err != nil {
		return 0, err
	}

	//処理時間計測
	fmt.Printf("Finish!! 経過(seconds): %vns\n", time.Since(cdt).Seconds())

	return resultsQty, nil
}

///////////////////////////////////////////////////
/* ===========================================
//ItemIdを利用してSkuのデータセットをレスポンスする

=========================================== */
func (res ResSku) GetSkuByItemId(w http.ResponseWriter, rq *REQ.RequestData) {

	var err error

	cdt := time.Now()

	//ItemIdをPostパラメーターから抽出
	var itemId string
	for _, p := range rq.PostParameter {
		if p.Name == "Query_ItemId" {
			itemId = p.StringValue
			break
		}
	}

	//Requestを格納
	var sr ENTITY.SkuRequest = ENTITY.SkuRequest{
		Items: itemId,
	}

	//Datastore clientを生成
	client, err := datastore.NewClient(context.Background(), CONFIG.GetConfig(CONFIG.PROJECT_ID))
	if err != nil {
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
		return
	}

	//ItemIdに紐づくSkuのEntityを取得する
	ens, _, err := qs.GetByItemId(rq, itemId, -1, client)
	if err != nil {
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
		return
	}
	ens1, ok := ens.(*[]ENTITY.EntitySku) //取得したEntityをCast
	if !ok {
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
		return
	}

	//処理時間計測
	fmt.Printf("Finish!! 経過(miliseconds): %vns\n", time.Since(cdt).Nanoseconds()/1000000)

	//Error Response
	if err != nil {
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
		return
	}

	//Response parse
	var sres ENTITY.EntitySkusResponse = ENTITY.EntitySkusResponse{
		Requests: sr,
		Sku:      *ens1}

	//API response
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode((sres))
}
