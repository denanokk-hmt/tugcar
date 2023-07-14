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
	DATASET_CHAINED_TAGS_LOGGING = "gcs_logging"                   //dataset
	TABLE_CHAINED_TAGS_LOGGING   = "t_chained_tags_logging_import" //tableId
)

// ログ格納
type LogChainedTagsOld struct {
	Log string
}

// ログ格納
type LogChainedTags struct {
	Log string
}

// テーブル定義
type ChainedTagsLoggingImport struct {
	Pdt             time.Time
	Cdt             time.Time
	ClientId        string
	CustomerUuid    string
	WhatYaId        string
	ATID            string
	DDID            int
	RelatedUnixtime int
	PublishedAt     string
	RelatedWordsLog [][]string //slice of slice
}

// Insert用のテーブル定義
type ChainedTagsLoggingImportInsert struct {
	Pdt             time.Time
	Cdt             time.Time
	ClientId        string
	CustomerUuid    string
	WhatYaId        string
	ATID            string
	DDID            int
	GID             string //[unixtime]_[index] :GroupID
	GQty            int    //:GroupIDの個数
	GSort           int
	RelatedUnixtime int
	PublishedAt     string
	RelatedWordsLog []string //slice
}

// データロード情報
type LoadChainedTagsLoggingImport struct {
	ATID string
	DDID int
}

// Search
type ChainedTagsLoggingImports struct {
	Records []ChainedTagsLoggingImport
}

///////////////////////////////////////////
//GCSから取得し、JSONデコードするための箱

// ///////////////////////////
// gcs::tugcar_chianed_tags_loggingへInsertする古いログが古いタイプ(~2022/9/*まで利用)→削除予定
type JsonPayloadOld struct {
	JsonPayload TextPayloadOld `json:"jsonPayload"`
}
type TextPayloadOld struct {
	TextPayload string `json:"textPayload"`
}

//gcs::tugcar_chianed_tags_loggingへInsertする古いログが古いタイプ(~2022/9/*まで利用)→削除予定
/////////////////////////////

type JsonPayload struct {
	JsonPayload TextPayload `json:"jsonPayload"`
}
type TextPayload struct {
	TextPayload string `json:"TextPayload"`
}
type JsonChainedTags struct {
	ClientId        string `json:"ClientId"`
	CustomerUuid    string `json:"CustomerUuid"`
	WhatYaId        string `json:"WhatYaId"`
	ATID            string `json:"ATID"`
	DDID            int    `json:"DDID"`
	RelatedUnixtime int    `json:"RelatedUnixtime"`
	PublishedAt     string `json:"PublishedAt"`
	//RelatedWordsLog RelatedWordsLogs `json:"RelatedWordsLog"`
}
type JsonRelatedWordsLogsValue [][]string
type JsonRelatedWordsLogs struct {
	RelatedWordsLogsValues JsonRelatedWordsLogsValue `json:"RelatedWordsLog"`
}

///////////////////////////////////////////////////
/* =========================================== */
// Chained Tags loggingログをパース
//gcs::tugcar_chianed_tags_loggingへInsertする古いログが古いタイプ(~2022/9/*まで利用)→削除予定
/* =========================================== */
func (l *LogChainedTagsOld) ChainedTagsLogParser() ([]ChainedTagsLoggingImport, error) {

	var err error
	var cs []ChainedTagsLoggingImport
	var cdt = time.Now()

	//1時間毎のログから、個別のログを分割(抽出)
	lp := strings.Replace(l.Log, "{\"insertId\":", "|{\"insertId\":", -1) //パイプで区切る
	lds := strings.Split(lp, "|")                                         //パイプをデリミタとして配列に分割
	lds = lds[1:]                                                         //パイプ置換で先頭が空要素になるため、これを除外する

	//ログから必要な文字列を抜き出す→JSON文字列をオブジェクトへUnmarshal
	for _, ld := range lds {

		//JsonPayloadに紐づく文字列をUnmarshal
		var jp JsonPayloadOld
		err = json.Unmarshal([]byte(ld), &jp)
		if err != nil {
			break
		}

		if jp.JsonPayload.TextPayload != "" {
			continue //新しいログに切換えた場合のフォーマットを認識→無視
		}

		//ログの中から"ChainedTagsLogging "以降の記述のみを抜き出す(Old対応)
		d := strings.Replace(ld, "ChainedTagsLogging ", "", 1)

		//TextPayloadに紐づく文字列をUnmarshal
		var tp TextPayloadOld
		err = json.Unmarshal([]byte(d), &tp)
		if err != nil {
			break
		}

		//ChainedTagsログをUnmarsal(RelatedWordsLog以外→配列なのでいっしょに出来ない)
		var ct JsonChainedTags
		err = json.Unmarshal([]byte(tp.TextPayload), &ct)
		if err != nil {
			break
		}

		//RelatedWordsLogをUnmarsal
		var rwls JsonRelatedWordsLogs
		err = json.Unmarshal([]byte(tp.TextPayload), &rwls)
		if err != nil {
			break
		}

		//Loggingされた時間(PublishedAt→Pdt)をTimesampに変換
		pa := strings.Split(ct.PublishedAt, ".")[0]
		pa = strings.Replace(pa, "-", "/", -1)
		pbt, _ := time.Parse("2006/01/02 15:04:05", pa)

		//Bq向けのレコードを作成
		var c ChainedTagsLoggingImport
		c.Pdt = pbt
		c.Cdt = cdt
		c.ClientId = ct.ClientId
		c.CustomerUuid = ct.CustomerUuid
		c.WhatYaId = ct.WhatYaId
		c.ATID = ct.ATID
		c.DDID = ct.DDID
		c.RelatedUnixtime = ct.RelatedUnixtime
		c.PublishedAt = ct.PublishedAt
		c.RelatedWordsLog = append(c.RelatedWordsLog, rwls.RelatedWordsLogsValues...)

		//返却箱に格納
		cs = append(cs, c)
	}

	//Loop内で起きたエラーをキャッチ
	if err != nil {
		fmt.Println(err)
		return cs, err
	}

	return cs, nil
}

///////////////////////////////////////////////////
/* =========================================== */
// Chained Tags loggingログをパース
/* =========================================== */
func (l *LogChainedTags) ChainedTagsLogParser() ([]ChainedTagsLoggingImport, error) {

	var err error
	var cs []ChainedTagsLoggingImport
	var cdt = time.Now()

	//1時間毎のログから、個別のログを分割(抽出)
	lp := strings.Replace(l.Log, "{\"insertId\":", "|{\"insertId\":", -1) //パイプで区切る
	lds := strings.Split(lp, "|")                                         //パイプをデリミタとして配列に分割
	lds = lds[1:]                                                         //パイプ置換で先頭が空要素になるため、これを除外する

	//ログから必要な文字列を抜き出す→JSON文字列をオブジェクトへUnmarshal
	for _, ld := range lds {

		//JsonPayloadに紐づく文字列をUnmarshal
		var jp JsonPayload
		err = json.Unmarshal([]byte(ld), &jp)
		if err != nil {
			break
		}

		//TextPayloadに紐づく文字列をUnmarshal
		var tp TextPayload = jp.JsonPayload

		//ChainedTagsログをUnmarsal(RelatedWordsLog以外→配列なのでいっしょに出来ない)
		var ct JsonChainedTags
		err = json.Unmarshal([]byte(tp.TextPayload), &ct)
		if err != nil {
			break
		}

		//RelatedWordsLogをUnmarsal
		var rwls JsonRelatedWordsLogs
		err = json.Unmarshal([]byte(tp.TextPayload), &rwls)
		if err != nil {
			break
		}

		//Loggingされた時間(PublishedAt→Pdt)をTimesampに変換
		pa := strings.Split(ct.PublishedAt, ".")[0]
		pa = strings.Replace(pa, "-", "/", -1)
		pbt, _ := time.Parse("2006/01/02 15:04:05", pa)

		//Bq向けのレコードを作成
		var c ChainedTagsLoggingImport
		c.Pdt = pbt
		c.Cdt = cdt
		c.ClientId = ct.ClientId
		c.CustomerUuid = ct.CustomerUuid
		c.WhatYaId = ct.WhatYaId
		c.ATID = ct.ATID
		c.DDID = ct.DDID
		c.RelatedUnixtime = ct.RelatedUnixtime
		c.PublishedAt = ct.PublishedAt
		c.RelatedWordsLog = append(c.RelatedWordsLog, rwls.RelatedWordsLogsValues...)

		//返却箱に格納
		cs = append(cs, c)
	}

	//Loop内で起きたエラーをキャッチ
	if err != nil {
		fmt.Println(err)
		return cs, err
	}

	return cs, nil
}
