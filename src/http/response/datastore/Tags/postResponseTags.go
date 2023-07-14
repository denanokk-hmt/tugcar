/*
======================
Datastoreの
Namespace: WhatYa-Attachment-[client]-[env]
kind: Tags
に対して、結果を取得しレスポンスをする
========================
*/
package response

import (
	"context"
	"fmt"
	"net/http"
	"time"

	BQ "bwing.app/src/bigquery/bq2ds"
	CONFIG "bwing.app/src/config" //共通Query
	"cloud.google.com/go/datastore"

	TQUERY "bwing.app/src/datastore/query/Tags" //Tags専用Query
	ERR "bwing.app/src/error"
	REQ "bwing.app/src/http/request"
)

// Inerface
type ResTags struct{}

var qt TQUERY.QueryTags

///////////////////////////////////////////////////
/* =========================================== */
//Tags データをBigQueryからロード
//SQLの拡張性を考慮して、マスタ別にこのResponseを準備する
/* =========================================== */
func (res ResTags) LoadTagsData(w http.ResponseWriter, r *http.Request, rq *REQ.RequestData) (int, error) {

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
//Tags ロードデータを更新
//更新処理の拡張性を考慮して、マスタ別にこのResponseを準備する
/* =========================================== */
func (res ResTags) UpdateTagsData(w http.ResponseWriter, r *http.Request, rq *REQ.RequestData, filters []REQ.Filter) (int, error) {

	cdt := time.Now()

	var err error

	//Datastore clientを生成(このClinetを使い回す)
	client, err := datastore.NewClient(context.Background(), CONFIG.GetConfig(CONFIG.PROJECT_ID))
	if err != nil {
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
		return 0, err
	}

	//Entityを更新:既存の有効Revisionを無効に更新(LatestRevision=FALSE)
	resultsQty, err := qt.UpdateLatestRevisionByRevision(rq, client, filters[:2], false)
	if err != nil {
		return 0, err
	}

	//Entityを更新:指定したRevisionを有効に更新(LatestRevision=TRUE)
	resultsQty, err = qt.UpdateLatestRevisionByRevision(rq, client, filters[2:], true)
	if err != nil {
		return 0, err
	}

	//処理時間計測
	fmt.Printf("Finish!! 経過(seconds): %vns\n", time.Since(cdt).Seconds())

	return resultsQty, nil
}
