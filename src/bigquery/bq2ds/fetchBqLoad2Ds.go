/*
======================
BigQueryからマスターデータを取得する処理を振り分ける
========================
*/
package bigquery

import (
	"context"
	"strings"

	CONFIG "bwing.app/src/config"
	ENTITY "bwing.app/src/datastore/entity"
	REQ "bwing.app/src/http/request"

	IFETCH "bwing.app/src/bigquery/bq2ds/Items"
	SFETCH "bwing.app/src/bigquery/bq2ds/Sku"
	TFETCH "bwing.app/src/bigquery/bq2ds/Tags"

	"cloud.google.com/go/bigquery"
)

///////////////////////////////////////////////////
/* ===========================================
//BogQueryからマスタのデータを取得してDatastoreへ挿入
* =========================================== */
func FetchBigQueryLoad2Ds(rq *REQ.RequestData) (int, error) {

	var err error
	var dataset string
	var qry string
	var it *bigquery.RowIterator
	var funcIf interface{}

	//Bqデータセット
	if rq.ParamsBasic.BqSource.Project != "" {
		//GCPのProjectを跨いでBigQueryデータを取得させたい場合
		//前提となるインフラの設定方法
		//https://svc.atlassian.net/wiki/spaces/CDLAB/pages/2302345274/Golang+Bigquery
		dataset = rq.ParamsBasic.BqSource.Project + "." + rq.ParamsBasic.BqSource.Dataset
	} else {
		//サーバーと同じGCPのプロジェクトのBigQueryデータを取得させる場合
		dataset = strings.ToLower(rq.ParamsBasic.ClientId)
	}

	//テーブルのSuffixは、Kindを小文字で指定(→Bq側の登録内容に準拠)
	table_suffix := strings.ToLower(rq.ParamsBasic.Kind)

	//BiqQueryのTableIDを形成
	viewId := dataset + ".bq2ds_" + table_suffix

	//SQLの基本部分
	q_SELECT := `SELECT * `
	q_FROM := "FROM `" + viewId + "`"

	//データ取得関数とSQL追加
	switch rq.ParamsBasic.Kind {
	case ENTITY.KIND_ITEMS: //BigQueryからDataをFetchし、DSのItemsへロードする場合

		//Itemsロード関数
		var f IFETCH.FetchItems
		funcIf = f

		//追加SQL
		q_WHERE := ""
		q_LIMIT := ""
		qry = q_SELECT + q_FROM + q_WHERE + q_LIMIT

	case ENTITY.KIND_SKU: //BigQueryからDataをFetchし、DSのSkuへロードする場合

		//Skuロード関数
		var f SFETCH.FetchSku
		funcIf = f

		//追加SQL
		q_WHERE := ""
		q_LIMIT := ""
		qry = q_SELECT + q_FROM + q_WHERE + q_LIMIT

	case ENTITY.KIND_TAGS: //BigQueryからDataをFetchし、DSのTagsへロードする場合

		//Tagsロード関数
		var f TFETCH.FetchTags
		funcIf = f

		//追加SQL
		q_WHERE := ""
		q_LIMIT := ""
		qry = q_SELECT + q_FROM + q_WHERE + q_LIMIT
	}

	//ItemIndex以外、BigQueryからレコードを取得
	if qry != "" {

		//BigQueryのclientを生成
		ctx := context.Background()
		client, err := bigquery.NewClient(ctx, CONFIG.GetConfig(CONFIG.PROJECT_ID))
		if err != nil {
			return 0, err
		}
		defer client.Close()

		//クエリを生成
		q := client.Query(qry)
		q.QueryConfig.UseStandardSQL = true

		//実行のためのqueryをサービスに送信してIteratorを通じて結果を返す
		it, err = q.Read(ctx)
		if err != nil {
			return 0, err
		}
	}

	/*------------------------------------------------
	Datastoreにデータをロード
	------------------------------------------------*/
	//結果の返却箱
	var resultsQty int

	switch rq.ParamsBasic.Kind {

	//BigQueryからDataをFetchし、DSのItemsへロード
	case ENTITY.KIND_ITEMS:
		//var f IFETCH.FetchItems
		//resultsQty, err = f.FetchBqLoad2Ds(rq, it)
		f := funcIf.(IFETCH.FetchItems)
		resultsQty, err = f.FetchBqLoad2Ds(rq, it)

	//BigQueryからDataをFetchし、DSのSkuへロード
	case ENTITY.KIND_SKU:
		//var f SFETCH.FetchSku
		//resultsQty, err = f.FetchBqLoad2Ds(rq, it)
		f := funcIf.(SFETCH.FetchSku)
		resultsQty, err = f.FetchBqLoad2Ds(rq, it)

	//BigQueryからDataをFetchし、DSのTagsへロード
	case ENTITY.KIND_TAGS:
		//var f TFETCH.FetchTags
		//resultsQty, err = f.FetchBqLoad2Ds(rq, it)
		f := funcIf.(TFETCH.FetchTags)
		resultsQty, err = f.FetchBqLoad2Ds(rq, it)
	}
	if err != nil {
		return 0, err
	}

	//結果
	return resultsQty, err
}
