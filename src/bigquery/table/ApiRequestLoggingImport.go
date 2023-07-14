/*
	=================================

BigQuery
Dataset::gcs_logging
Table::t_chained_tags_logging_import
テーブルの構造をここで指定する
* =================================
*/
package table

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

///////////////////////////////////////////////////
/* ===========================================
Table::
* =========================================== */

var (
	DATASET_API_REQUEST_LOGGING = "gcs_logging"                  //dataset
	TABLE_API_REQUEST_LOGGING   = "t_api_request_logging_import" //tableId
)

// ログ格納
type LogApiRequest struct {
	Log string
}

// テーブル定義
type ApiRequestLoggingImport struct {
	Cdt           time.Time `bigquery:"cdt"`
	Urlpath       string    `bigquery:"url_path"`
	Type          string    `bigquery:"type"`
	ClientId      string    `bigquery:"client_id"`
	CurrentParams string    `bigquery:"current_params"`
	CurrentUrl    string    `bigquery:"current_url"`
	Query_Tags    string    `bigquery:"query_tags"`
	Query_ItemId  string    `bigquery:"query_item_id"`
	CustomerUuid  string    `bigquery:"customer_uuid"`
	WhatYaId      string    `bigquery:"hmt_id"`
	SearchFrom    string    `bigquery:"search_from"`
	Timestamp     time.Time `bigquery:"timestamp"`
}

// Insert用のテーブル定義
type ApiRequestLoggingImportInsert struct {
	Pdt          time.Time
	Cdt          time.Time
	ClientId     string
	CustomerUuid string
	WhatYaId     string
	PublishedAt  string
}

// Search
type ApiRequestLoggingImports struct {
	Records []ApiRequestLoggingImport
}

// /////////////////////////////////////////
// GCSからバケットを取得し、JSON文字列をデコードするための箱
type JsonBucketAR struct {
	InserId     string        `json:"insertId"`
	JsonPayload JsonPayloadAR `json:"jsonPayload"`
	Timestamp   string        `json:"timestamp"`
}
type JsonPayloadAR struct {
	TextPaylaod string `json:"textPayload"`
	Timestamp   string `json:"timestamp"`
}
type JsonTextPayloadAR struct {
	Params ParamsAR `json:"Params"`
}
type ParamsAR struct {
	Type          string
	ClientId      string
	CurrentParams string
	CurrentUrl    string
	Query_Tags    string
	Query_ItemId  string
	UserAgent     string
	Referrer      string
	CustomerUuid  string
	WhatyaId      string
	SearchFrom    string
}

///////////////////////////////////////////////////
/* =========================================== */
// Chained Tags loggingログをパース
/* =========================================== */
func (l *LogApiRequest) ApiRequestLogParser() ([]ApiRequestLoggingImport, error) {

	var err error
	var ars []ApiRequestLoggingImport
	var cdt = time.Now()

	//1時間毎のログから、個別のログを分割(抽出)
	lp := strings.Replace(l.Log, "{\"insertId\":", "|{\"insertId\":", -1) //パイプで区切る
	lds := strings.Split(lp, "|")                                         //パイプをデリミタとして配列に分割
	lds = lds[1:]                                                         //パイプ置換で先頭が空要素になるため、これを除外する

	//ログから必要な文字列を抜き出す→JSON文字列をオブジェクトへUnmarshal
	for _, ld := range lds {

		//JsonPayloadに紐づく文字列をUnmarshal
		var jb JsonBucketAR
		err = json.Unmarshal([]byte(ld), &jb)
		if err != nil {
			break
		}

		//jsonPayload.textPayloadの中の" Params:"前後で切り出し
		tp := strings.Split(jb.JsonPayload.TextPaylaod, " Params:")

		//Urlpathを取り出す
		var urlPath string
		rArr := strings.Split(tp[0], " ")
		for _, r := range rArr {
			kv := strings.Split(r, ":")
			if len(kv) == 2 {
				if kv[0] == "Urlpath" {
					urlPath = kv[1]
					break
				}
			}
		}
		if urlPath == "" {
			continue
		}

		if strings.Contains(urlPath, "SpecialTag") {
			fmt.Println(urlPath)
		}

		//jsonPayload.textPayloadの中のParams後ろの切り出し-->Paramsを格納へ
		pArr := strings.Split(tp[1], ",")

		//切り出したParamsをKey&Valueにマッピング
		paramsMapper := make(map[string]string)
		for _, p := range pArr {
			kv := strings.Split(p, ":")
			if len(kv) == 2 {
				paramsMapper[kv[0]] = kv[1]
			} else if len(kv) == 3 {
				paramsMapper[kv[0]] = kv[1] + ":" + kv[2]
			}
		}

		//Timesampを型変換
		ts, _ := time.Parse("2006-01-02T15:04:05Z07:00", jb.Timestamp)

		//Bq向けのレコードを作成
		var ar ApiRequestLoggingImport = ApiRequestLoggingImport{
			Cdt:           cdt,
			Urlpath:       urlPath,
			Type:          paramsMapper["Type"],
			ClientId:      paramsMapper["ClientId"],
			CurrentParams: paramsMapper["CurrentParams"],
			CurrentUrl:    paramsMapper["CurrentUrl"],
			Query_Tags:    paramsMapper["Query_Tags"],
			Query_ItemId:  paramsMapper["Query_ItemId"],
			CustomerUuid:  paramsMapper["CustomerUuid"],
			WhatYaId:      paramsMapper["WhatYaId"],
			SearchFrom:    paramsMapper["SearchFrom"],
			Timestamp:     ts,
		}

		//返却箱に格納
		ars = append(ars, ar)
	}

	//Loop内で起きたエラーをキャッチ
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	return ars, nil
}
