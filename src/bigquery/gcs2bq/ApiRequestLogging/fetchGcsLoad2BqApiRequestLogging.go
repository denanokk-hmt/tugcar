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
type FetchGcsApiRequestLogging struct{}

///////////////////////////////////////////////////
/* ===========================================
//取得したGCS BuketデータをBigQueryへロードする
* =========================================== */
func (f FetchGcsApiRequestLogging) FetchGcs(rq *REQ.RequestData, bucket, middlePath string, sArr []string) (interface{}, int, error) {

	var err error

	/*------------------------------------------------
	GCS BucketからApiRequestのログを取得
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
	var pRecords []TABLE.ApiRequestLoggingImport

	//ログをパース
	for i, logs := range logss.objs {
		for ii, log := range logs {

			//ApiRequestLoggingログをパース(log=1時間分のものがやってくる)
			var hRecords []TABLE.ApiRequestLoggingImport

			l := &TABLE.LogApiRequest{Log: log}
			hRecords, err = l.ApiRequestLogParser()
			if err != nil {
				fmt.Println(err)
				return nil, 0, err
			}

			pRecords = append(pRecords, hRecords...)

			//GCS 取得結果
			var result TABLE.BqLoadResults = TABLE.BqLoadResults{
				Result:  0,
				Cdt:     cdt,
				LogNo:   i,
				LogPath: logss.paths[i][ii],
				LogDate: logss.logDate[i],
				TTL:     int(ttl),
			}
			fmt.Println(result)
			load_results = append(load_results, result)
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
	rc, err := load2BqLogs(rq, pRecords, bqClient, &bqCtx)
	if err != nil {
		return nil, 0, err
	}

	return &load_results, rc, err
}

///////////////////////////////////////////////////
/* =========================================== */
//BigQueryに、LogsをLoadする
/* =========================================== */
func load2BqLogs(rq *REQ.RequestData, records []TABLE.ApiRequestLoggingImport, bqClient *bigquery.Client, bqCtx *context.Context) (int, error) {

	//for local testing
	if BqNoInsert {
		fmt.Println("【【【 BqNoInsert is TRUE 】】】")
		return 0, nil
	}

	//BiqQueryのTableID情報
	dataset := TABLE.DATASET_API_REQUEST_LOGGING
	table := TABLE.TABLE_API_REQUEST_LOGGING

	//チャンクを計算
	var rss [][]TABLE.ApiRequestLoggingImport
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
