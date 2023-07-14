/*
======================
Datastoreの
Namespace: WhatYa-Attachment-[client]-[env]
kind: ItemIndex
に対して、結果を取得しレスポンスをする
========================
*/
package response

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"cloud.google.com/go/datastore"

	IXFETCH "bwing.app/src/bigquery/bq2ds/ItemIndex"
	CONFIG "bwing.app/src/config"                     //共通Query
	IXQUERY "bwing.app/src/datastore/query/ItemIndex" //ItemIndex専用Query
	ERR "bwing.app/src/error"
	REQ "bwing.app/src/http/request"
)

// Inerface
type ResItemIndex struct{}

// Query
var qix IXQUERY.QueryItemIndex

///////////////////////////////////////////////////
/* ===========================================
//DSへロード済みのItemsとTagsのデータからWorChainを形成し、ItemIndexへロード
=========================================== */
func (res ResItemIndex) CreateWordChainAndLoadItemIndex(w http.ResponseWriter, r *http.Request, rq *REQ.RequestData) (int, error) {

	cdt := time.Now()
	/*
		//BigQueryのDataMartから取得したデータを、Datastoreにロードする
		resultsQty, err := BQ.FetchBigQueryLoad2Ds(rq, "")
		if err != nil {
			return 0, err
		}
	*/
	//ItemsとTagsからWordChainを形成し、ItemIndexへロード
	var f IXFETCH.ResItemIndex
	resultsQty, err := f.Load2DsItemIdIndex(rq)
	if err != nil {
		return 0, err
	}

	//処理時間計測
	fmt.Printf("Finish!! 経過(seconds): %vns\n", time.Since(cdt).Seconds())

	return resultsQty, nil
}

///////////////////////////////////////////////////
/* =========================================== */
//ItemIndex ロードデータを更新
//更新処理の拡張性を考慮して、マスタ別にこのResponseを準備する
/* =========================================== */
func (res ResItemIndex) UpdateItemIndexData(w http.ResponseWriter, r *http.Request, rq *REQ.RequestData, filters []REQ.Filter) (int, error) {

	cdt := time.Now()

	var err error

	//Datastore clientを生成(このClinetを使い回す)
	client, err := datastore.NewClient(context.Background(), CONFIG.GetConfig(CONFIG.PROJECT_ID))
	if err != nil {
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
		return 0, err
	}

	//Entityを更新:既存の有効Revisionを無効に更新(LatestRevision=FALSE)
	_, err = qix.UpdateLatestRevisionByRevision(rq, client, filters[:2], false)
	if err != nil {
		return 0, err
	}

	//Entityを更新:指定したRevisionを有効に更新(LatestRevision=TRUE)
	resultsQty, err := qix.UpdateLatestRevisionByRevision(rq, client, filters[2:], true)
	if err != nil {
		return 0, err
	}

	//処理時間計測
	fmt.Printf("Finish!! 経過(seconds): %vns\n", time.Since(cdt).Seconds())

	return resultsQty, nil
}
