/*
======================
GCSからバケットファイルを取得し、BigQueryへLoadする
========================
*/
package bigquery

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	TABLE "bwing.app/src/bigquery/table"
	COMMON "bwing.app/src/common"
	GCS "bwing.app/src/gcs"
	REQ "bwing.app/src/http/request"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
)

// BigQueryへのレコードインサートをさせない(=true)
var BqNoInsert bool = false

// Inerface
type FetchGcsChainedTagsLogging struct{}

///////////////////////////////////////////////////
/* ===========================================
//取得したGCS BuketデータをBigQueryへロードする
* =========================================== */
func (f FetchGcsChainedTagsLogging) FetchGcs(rq *REQ.RequestData, bucket, middlePath string, sArr []string) (interface{}, int, error) {

	var err error

	/*------------------------------------------------
	GCS BucketからChainedTagsのログを取得
	------------------------------------------------*/

	//GCS clientを生成
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer client.Close()

	type logssStruct struct {
		objs    [][]string
		paths   [][]string
		logDate []string
	}
	var logss logssStruct
	var logLen int
	for i, s := range sArr {

		//読み取りたいログのパスを取得
		prefix := middlePath + "/" + s
		fmt.Println("Bucket getting:", bucket, prefix)
		paths, ctx, err := GCS.ObjectPathGetter(client, bucket, prefix)
		if err != nil {
			fmt.Println(ctx.Err())
			return nil, 0, err
		}
		//Bucketのログデータ取得
		logs, ctx, err := GCS.ObjectReader(client, bucket, paths)
		if err != nil {
			fmt.Println(ctx.Err())
			return nil, 0, err
		}
		if len(logs) != 0 {
			logLen += len(logs)
			logss.objs = append(logss.objs, logs)
			logss.paths = append(logss.paths, paths)
			logss.logDate = append(logss.logDate, sArr[i])
		}
	}

	if logLen == 0 {
		err = errors.New("Data does not exist.")
		return nil, 0, err
	}

	/*------------------------------------------------
	ログをパース
	------------------------------------------------*/

	//取得レコード数を確保
	ttl := logLen

	cdt := time.Now()

	//結果箱の準備
	var load_results []TABLE.BqLoadResults

	//パースしたすべてのログを平滑に入れておく箱
	var pRecords []TABLE.ChainedTagsLoggingImport

	//ログをパース
	for i, logs := range logss.objs {
		for ii, log := range logs {

			//ChainedTagsLoggingログをパース(log=1時間分のものがやってくる)
			var hRecords []TABLE.ChainedTagsLoggingImport

			if bucket == "tugcar_chianed_tags_logging" {
				//gcs::tugcar_chianed_tags_loggingへInsertする古いログが古いタイプ(~2022/9/*まで利用)→削除予定
				l := &TABLE.LogChainedTagsOld{Log: log}
				hRecords, err = l.ChainedTagsLogParser()
			} else if bucket == "chained_tags_logging" {
				l := &TABLE.LogChainedTags{Log: log}
				hRecords, err = l.ChainedTagsLogParser()
			}
			if err != nil {
				fmt.Println(err)
				return nil, 0, err
			}

			pRecords = append(pRecords, hRecords...)

			//GCS 取得結果
			var result TABLE.BqLoadResults = TABLE.BqLoadResults{
				Result:  0,
				Client:  rq.ParamsBasic.ClientId,
				Cdt:     cdt,
				LogNo:   i,
				LogPath: logss.paths[i][ii],
				LogDate: logss.logDate[i],
				TTL:     int(ttl),
			}
			fmt.Println(result, log)
			load_results = append(load_results, result)
		}
	}

	//取得したログをATID-DDID別にマッピングする
	var adGroupMap = make(map[string][]TABLE.ChainedTagsLoggingImport)
	for _, pr := range pRecords {
		adGroupMap[pr.ATID+"ddid-"+strconv.Itoa(pr.DDID)] = append(adGroupMap[pr.ATID+"ddid-"+strconv.Itoa(pr.DDID)], pr)
	}

	//ATID-DDID別マッピングしたログの中で、一番最後の要素のみに選別
	//*ChainedTagsのロギング方法として、TagsWordを配列に配列を入れて引き継ぐ
	//*よってATID-DDID別の中で、最後のログがすべてのTagsWordを選択された順番で
	//*配列of配列の形でもっているため
	var lRecords []TABLE.ChainedTagsLoggingImport
	for _, m := range adGroupMap {
		fmt.Println(m)
		lRecords = append(lRecords, m[len(m)-1])
	}

	//選別後のInsert用の箱にたたみ直し
	var iRecords []*TABLE.ChainedTagsLoggingImportInsert
	for i, lr := range lRecords {
		groupId := time.Now().Unix() ////group id用のunixtimeを発行
		gqty := len(lr.RelatedWordsLog)
		for ii, rw := range lr.RelatedWordsLog {
			var iRecord TABLE.ChainedTagsLoggingImportInsert
			iRecord.Pdt = lr.Pdt
			iRecord.Cdt = lr.Cdt
			iRecord.ClientId = lr.ClientId
			iRecord.CustomerUuid = lr.CustomerUuid
			iRecord.WhatYaId = lr.WhatYaId
			iRecord.ATID = lr.ATID
			iRecord.DDID = lr.DDID
			iRecord.GID = strconv.Itoa(int(groupId)) + "_" + strconv.Itoa(i) + "_" + strconv.Itoa(ii)
			iRecord.GQty = gqty
			iRecord.GSort = ii
			iRecord.RelatedUnixtime = lr.RelatedUnixtime
			iRecord.PublishedAt = lr.PublishedAt
			iRecord.RelatedWordsLog = rw
			iRecords = append(iRecords, &iRecord)
		}
	}

	/*------------------------------------------------
	BigQueryへログをインサート
	------------------------------------------------*/

	//BigQueryのclientを生成
	projectId := TABLE.GetProjectId()
	bqCtx := context.Background()
	bqClient, err := bigquery.NewClient(bqCtx, projectId)
	if err != nil {
		return nil, 0, err
	}

	//BqにBulk Insertを行う
	rc, err := load2BqLogs(rq, iRecords, bqClient, &bqCtx)
	if err != nil {
		return nil, 0, err
	}

	return &load_results, rc, err
}

///////////////////////////////////////////////////
/* =========================================== */
//BigQueryに、LogsをLoadする
/* =========================================== */
func load2BqLogs(rq *REQ.RequestData, records []*TABLE.ChainedTagsLoggingImportInsert, bqClient *bigquery.Client, bqCtx *context.Context) (int, error) {

	//for local testing
	if BqNoInsert {
		fmt.Println("【【【 BqNoInsert is TRUE 】】】")
		return 0, nil
	}

	//BiqQueryのTableID情報
	dataset := TABLE.DATASET_CHAINED_TAGS_LOGGING
	table := TABLE.TABLE_CHAINED_TAGS_LOGGING

	//チャンクを計算
	var rss [][]*TABLE.ChainedTagsLoggingImportInsert
	chunks := COMMON.ChunkCalculator2(len(records), 10000)
	for _, c := range chunks.Positions {
		rs := records[c.Start:c.End]
		rss = append(rss, rs)
	}

	//Bqへバルクインサート
	for _, rs := range rss {
		u := bqClient.Dataset(dataset).Table(table).Uploader()
		err := u.Put(*bqCtx, rs)
		if err != nil {
			return 0, err
		}
	}

	return len(records), nil
}
