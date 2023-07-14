/*
======================
Logging
========================
*/
package log

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"time"

	COMMON "bwing.app/src/common"
	CONFIG "bwing.app/src/config"
	ENTITY "bwing.app/src/datastore/entity"
	TQUERY "bwing.app/src/datastore/query/Tags"
	REQ "bwing.app/src/http/request"
	"cloud.google.com/go/datastore"
	"github.com/pkg/errors"
)

var (
	INFO  = "INFO"
	WARN  = "WARNING"
	ERROR = "ERROR"
)

type LogEntry struct {
	Severity    string    `json:"severity"`    //ログレベル
	LogName     string    `json:"logName"`     //ログ名
	TextPayload string    `json:"textPayload"` //ログ内容
	Timestamp   time.Time `json:"timestamp"`   //ログタイムスタンプ
}

type OsSettingsStruct struct {
	Hostname string
}

var OsSettings OsSettingsStruct

type RequestLogs struct {
	Host    string
	Method  string
	Urlpath string
	Headers http.Header
	Params  string
}

type Attachementlog struct {
	ChainedTags ChainedTags
	SearchFrom  string
}
type ChainedTags struct {
	ClientId        string
	CustomerUuid    string
	WhatYaId        string
	ATID            string
	DDID            int
	RelatedUnixtime int
	RelatedWordsLog [][]string
	SelectedItem    SelectedItem
	PublishedAt     string
	Type            string
}
type SelectedItem struct {
	Id   string
	Name string
}

func init() {
	n, _ := os.Hostname()
	OsSettings.Hostname = n
	//fmt.Println(OsSettings)
}

func init() {
	log.SetPrefix("") // 接頭辞の設定
}

///////////////////////////////////////////////////
/* =========================================== */
//構造体をJSON形式の文字列へ変換
/* =========================================== */
func (l LogEntry) String() string {
	out, err := json.Marshal(l)
	if err != nil {
		log.Printf("json.Marshal: %v", err)
	}
	return string(out)
}

// ログエントリの箱につめる
func SetLogEntry(level, logName, text string) string {
	entry := &LogEntry{
		Severity:    level,
		LogName:     logName,
		TextPayload: text,
		Timestamp:   time.Now(),
	}
	return entry.String()
}

///////////////////////////////////////////////////
/* =========================================== */
//API Request Logging
/* =========================================== */
func ApiRequestLogging(rq *REQ.RequestData, level string) {

	//出力項目
	var output RequestLogs = RequestLogs{
		Host:    OsSettings.Hostname,
		Method:  rq.Method,
		Urlpath: rq.Urlpath,
		Headers: rq.Header.(http.Header),
		Params:  rq.ParamsBasic.ParamsStrings,
	}

	//リクエストをロギング
	fmt.Println(SetLogEntry(level, "ApiRequestLogging", fmt.Sprintf("%+v", output)))
}

///////////////////////////////////////////////////
/* ===========================================
Attachmentロギング
=========================================== */
func AttachmentLogging(rq *REQ.RequestData, ch chan bool) {

	defer close(ch)

	/*-------------------------
	ログを拾う
		ChaingedTagsLogging
		SearchFrom
		SelectedItem
	-------------------------*/

	var outputCT ChainedTags
	var outputSF string

	//AttachmentIDが無い場合は、ログ対象外
	for _, p := range rq.PostParameter {
		switch p.Name {
		case "ChainedTags_ATID":
			outputCT.ATID = p.StringValue
			if outputCT.ATID == "" {
				return
			}
			goto LOGGING
		}
	}
LOGGING:
	//出力項目
	for _, p := range rq.PostParameter {
		switch p.Name {
		case "ClientId":
			outputCT.ClientId = p.StringValue
		case "CustomerUuid":
			outputCT.CustomerUuid = p.StringValue
		case "WhatYaId":
			outputCT.WhatYaId = p.StringValue
		case "ChainedTags_ATID":
			outputCT.ATID = p.StringValue
		case "ChainedTags_DDID":
			outputCT.DDID = p.IntValue
		case "ChainedTags_RelatedUnixtime":
			outputCT.RelatedUnixtime = p.IntValue
		case "ChainedTags_RelatedWordsLog":
			//TagsWordが、ItemIdタグのみだった場合、紐づく他のTagsWordを入れ込む
			if len(p.StringArrayArray) == 1 && len(p.StringArrayArray[0]) == 1 {
				itemId, err := strconv.Atoi(p.StringArrayArray[0][0])
				if err == nil {
					ens, err := getTagsWords(rq, strconv.Itoa(itemId))
					if err != nil {
						fmt.Println(err) //エラーは握りつぶして、他のログ出力や処理を優先
					}
					p.StringArrayArray = ens
				}
			}
			outputCT.RelatedWordsLog = p.StringArrayArray
		case "SearchFrom":
			outputSF = p.StringValue
		case "ChainedTags_SelectedItem_Id":
			outputCT.SelectedItem.Id = p.StringValue
		case "ChainedTags_SelectedItem_Name":
			outputCT.SelectedItem.Name = p.StringValue
		case "PublishedAt":
			outputCT.PublishedAt = p.TimeValue.String()
		}
	}

	//Typeを設定(Ds Kindを設定)
	outputCT.Type = rq.ParamsBasic.Kind

	/*-------------------------
	ログ出力
	-------------------------*/

	//削除予定＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝2023/1/1~
	//ChainedTagsLogging
	//上で作った出力項目をjsonエンコード
	jsonCT, err := json.Marshal(outputCT)
	if err != nil {
		panic(err)
	}

	//JSON文字列化
	jsonStringCT := string(jsonCT)

	//構造化に組み入れてロギング
	fmt.Println(SetLogEntry(INFO, "ChainedTagsLogging", fmt.Sprintf("%+v\n", jsonStringCT)))
	//削除予定＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝2023/1/1~

	//Attachmentログとして固める
	var outputAT Attachementlog = Attachementlog{
		ChainedTags: outputCT,
		SearchFrom:  outputSF,
	}
	//jsonエンコード
	jsonAT, err := json.Marshal(outputAT)
	if err != nil {
		panic(err)
	}
	//JSON文字列化
	jsonStringAT := string(jsonAT)
	fmt.Println(SetLogEntry(INFO, "AttachmentLogging", fmt.Sprintf("%+v\n", jsonStringAT)))

	ch <- true
}

///////////////////////////////////////////////////
/* ===========================================
ItemIdタグを使ってすべてのTagsWordを取得する
=========================================== */
func getTagsWords(rq *REQ.RequestData, itemId string) ([][]string, error) {

	var qt TQUERY.QueryTags

	//Datastore clientを生成
	client, err := datastore.NewClient(context.Background(), CONFIG.GetConfig(CONFIG.PROJECT_ID))
	if err != nil {
		fmt.Printf("Error create datastore client.: %v", err)
		return nil, errors.WithStack(err)
	}

	//TagsをItemIdで検索してTagsWordを取得
	entity, err := qt.GetTagsWordByItemId(rq, itemId, nil, client)
	if err != nil {
		return nil, err
	}
	tws, _ := entity.(*[]ENTITY.EntityTags) //取得したEntityをCast

	//TagsWordを詰め直す
	var rwss [][]string
	var rws []string
	for _, t := range *tws {
		rws = append(rws, t.TagsWord)
	}

	//データエラーケースケア(ログ爆発を抑える)
	//データの重複があった場合、これを抑える
	rws = COMMON.RemoveDuplicateArrayString(rws)

	//出力するTagWordsをたたむ
	rwss = append(rwss, rws)

	//ItemIdタグが先頭になるようにソート
	sort.SliceStable(rwss[0], func(i, j int) bool { return rwss[0][i] < rwss[0][j] })

	return rwss, nil
}
