/*
======================
POSTリクエストされたパラメーターの受信
JsonDataを処理
========================
*/
package postdata

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	COMMON "bwing.app/src/common"
	ENTITY "bwing.app/src/datastore/entity"
	REQ "bwing.app/src/http/request"
)

// /////////////////////////////////////////////////
func ParseJson(r *http.Request, rq *REQ.RequestData) ([]uint8, int, error) {

	var err error

	//Validation
	if rq.Method == "GET" {
		err = errors.New("Method")
		return nil, http.StatusBadRequest, err
	}
	if r.Header.Get("Content-Type") != "application/json" {
		err = errors.New("Content-Type")
		return nil, http.StatusBadRequest, err
	}
	//To allocate slice for request body
	length, err := strconv.Atoi(r.Header.Get("Content-Length"))
	if err != nil {
		err = errors.New("Content-Length")
		return nil, http.StatusInternalServerError, err
	}

	//Read body data to parse json
	body := make([]byte, length)
	length, err = r.Body.Read(body)
	if err != nil && err != io.EOF {
		err = errors.New("")
		return nil, http.StatusInternalServerError, err
	}

	return body, length, nil
}

// /////////////////////////////////////////////////
func ParseJsonData(r *http.Request, rq *REQ.RequestData) error {

	//parse json request body
	body, length, err := ParseJson(r, rq)
	if err != nil {
		return err
	}

	//parse json
	var jsonBody map[string]interface{}
	err = json.Unmarshal(body[:length], &jsonBody)
	if err != nil {
		err = errors.New("Cannot unmarshal JSON.")
		return err
	}

	fmt.Printf("%v\n", jsonBody)

	//元の型へキャストしてPrameterへ格納、連想配列が"IDKey"or"NameKey"の場合は、Keyへ格納
	var p REQ.PostParameter
	for n, v := range jsonBody {
		switch v := v.(type) {
		case string:
			fmt.Printf("%s:%s (%T)\n", n, v, v)
			s := jsonBody[n].(string)
			p = REQ.PostParameter{Name: n, Type: "string", StringValue: s}
		case int:
			fmt.Printf("%s:%d (%T)\n", n, v, v)
			i := int(jsonBody[n].(int))
			p = REQ.PostParameter{Name: n, Type: "int", IntValue: i}
		case int64:
			fmt.Printf("%s:%d (%T)\n", n, v, v)
			i64 := int64(jsonBody[n].(int64))
			p = REQ.PostParameter{Name: n, Type: "int64", Int64Value: i64}
		case float32:
			fmt.Printf("%s:%f (%T)\n", n, v, v)
			f32 := float32(jsonBody[n].(float32))
			p = REQ.PostParameter{Name: n, Type: "float32", Float32Value: f32}
		case float64:
			fmt.Printf("%s:%f (%T)\n", n, v, v)
			f64 := float64(jsonBody[n].(float64))
			p = REQ.PostParameter{Name: n, Type: "float64", Float64Value: f64}
		case bool:
			fmt.Printf("%s:%t (%T)\n", n, v, v)
			b := jsonBody[n].(bool)
			p = REQ.PostParameter{Name: n, Type: "bool", BoolValue: b}
		//case Robot:
		//	fmt.Printf("%s: %+v (%T)\n", i, v, v) // valStruct: {name:Doraemon birth:2112} (main.Robot)
		default:
			fmt.Println(p)
			fmt.Printf("I don't know about type %s %T!\n", n, v)
		}
		//格納
		rq.PostParameter = append(rq.PostParameter, p)
	}
	//fmt.Println(rq)

	return nil
}

// /////////////////////////////////////////////////
func ParseArrayJsonData(r *http.Request, rq *REQ.RequestData) error {

	//parse json request body
	body, _, err := ParseJson(r, rq)
	if err != nil {
		return err
	}

	//parse json array
	var jsonBody []ENTITY.EntityArticleJson
	if err := json.Unmarshal(body, &jsonBody); err != nil {
		log.Fatal(err)
	}

	ks := []ENTITY.KeyArticle{}
	ens := []ENTITY.EntityArticle{}

	for _, v := range jsonBody {

		//Key
		ka := ENTITY.KeyArticle{ID: 0, Name: v.NameKey}
		ks = append(ks, ka)

		en := ENTITY.EntityArticle{
			Body:        v.Body,
			Content:     v.Content,
			Title:       v.Title,
			Number:      v.Number,
			PublishedAt: time.Now()}
		ens = append(ens, en)
	}

	return nil
}

///////////////////////////////////////////////////
/* ===========================================
【LoadAPI】
Attachemnt Load data
データセットをQueryする際に送信されてくるPostDataを格納する構造体
=========================================== */
type LoadPostData struct {
	Type                 string     `json:"type"`
	ClientId             string     `json:"client_id"`
	LatestRevision       bool       `json:"latest_revision"`
	ItemsRevision        int        `json:"items_revision"` //load Tagsのときに利用
	TagsRevision         int        `json:"tags_revision"`  //load ItemIndexのときに利用
	ForceBqSource        Bq         `json:"force_bq_source"`
	DepthCutter          DepthValue `json:"depth_cutter"`           //ItemIndex用
	SearchTagsWordTiedUp bool       `json:"search_tagsword_tideup"` //ItemIndex用
	MaxItemIndexQty      int        `json:"max_itemindex_qty"`      //ItemIndex用
	StartDate            int        `json:"start_date"`
	EndDate              int        `json:"end_date"`
	Revision             int        `json:"revision"`
	BqNoInsert           bool       `json:"bq_no_insert"`
}
type Bq struct {
	BqProject string `json:"project"`
	BqDataset string `json:"dataset"`
}
type DepthValue struct {
	Type  string  `json:"type"`
	Ope   string  `json:"ope"`
	Rate  float32 `json:"rate"`
	Level int     `json:"level"`
}

///////////////////////////////////////////////////
/* ===========================================
【LoadAPI】
LoadAPIリクエストのPOSTパラメーターを取得
=========================================== */
func ParseAttachmentLoadJsonData(w http.ResponseWriter, r *http.Request, rq *REQ.RequestData) error {

	//parse json request body
	body, length, err := ParseJson(r, rq)
	if err != nil {
		return err
	}

	//parse json
	var jsonBody LoadPostData
	err = json.Unmarshal(body[:length], &jsonBody)
	if err != nil {
		err = errors.New(err.Error())
		return err
	}

	var s string //ParamsStringsの箱

	//reflectを使って構造体の中身を探索しつつ、Parameterを格納
	var p REQ.PostParameter
	rtCst := reflect.TypeOf(jsonBody)
	rvCst := reflect.ValueOf(jsonBody)
	for i := 0; i < rtCst.NumField(); i++ {
		f := rtCst.Field(i) // フィールド情報を取得
		v := rvCst.FieldByName(f.Name).Interface()
		switch f.Name {
		case "LatestRevision":
			p = REQ.PostParameter{Name: "LatestRevision", Type: "BoolValue", BoolValue: v.(bool)}
			rq.PostParameter = append(rq.PostParameter, p)
		case "ForceBqSource":
			p = REQ.PostParameter{Name: "BqProject", Type: "StringValue", StringValue: v.(Bq).BqProject}
			rq.PostParameter = append(rq.PostParameter, p)
			s = s + f.Name + "_BqProject:" + v.(Bq).BqProject + ","
			p = REQ.PostParameter{Name: "BqDataset", Type: "StringValue", StringValue: v.(Bq).BqDataset}
			rq.PostParameter = append(rq.PostParameter, p)
			s = s + f.Name + "_BqDataset:" + v.(Bq).BqProject + ","
		case "DepthCutter":
			p = REQ.PostParameter{Name: "DepthCutter_Type", Type: "StringValue", StringValue: v.(DepthValue).Type}
			rq.PostParameter = append(rq.PostParameter, p)
			s = s + "DepthCutter_Type:" + v.(DepthValue).Type + ","
			p = REQ.PostParameter{Name: "DepthCutter_Ope", Type: "StringValue", StringValue: v.(DepthValue).Ope}
			rq.PostParameter = append(rq.PostParameter, p)
			s = s + "DepthCutter_Ope:" + v.(DepthValue).Ope + ","
			if v.(DepthValue).Type != "Ave" {
				p = REQ.PostParameter{Name: "DepthCutter_Rate", Type: "Float32Value", Float32Value: v.(DepthValue).Rate}
				rq.PostParameter = append(rq.PostParameter, p)
				f32 := v.(DepthValue).Rate
				s = s + "DepthCutter_Rate:" + strconv.FormatFloat(float64(f32), 'f', 2, 32) + ","
			}
			if v.(DepthValue).Level != 0 {
				p = REQ.PostParameter{Name: "DepthCutter_Level", Type: "IntValue", IntValue: v.(DepthValue).Level}
				rq.PostParameter = append(rq.PostParameter, p)
				s = s + "DepthCutter_Level:" + strconv.Itoa(int(v.(DepthValue).Level)) + ","
			}
		case "SearchTagsWordTiedUp", "BqNoInsert":
			p = REQ.PostParameter{Name: f.Name, Type: "BoolValue", BoolValue: v.(bool)}
			rq.PostParameter = append(rq.PostParameter, p)
			s = s + f.Name + strconv.FormatBool(v.(bool)) + ","
		case "MaxItemIndexQty":
			p = REQ.PostParameter{Name: "MaxItemIndexQty", Type: "IntValue", IntValue: v.(int)}
			rq.PostParameter = append(rq.PostParameter, p)
			s = s + f.Name + strconv.Itoa(v.(int)) + ","
		case "StartDate", "EndDate", "Revision", "ItemsRevision", "TagsRevision":
			p = REQ.PostParameter{Name: f.Name, Type: "IntValue", IntValue: v.(int)}
			rq.PostParameter = append(rq.PostParameter, p)
			s = s + f.Name + ":" + strconv.Itoa(v.(int)) + ","
		default:
			p = REQ.PostParameter{Name: f.Name, Type: "StringValue", StringValue: v.(string)}
			rq.PostParameter = append(rq.PostParameter, p)
			s = s + f.Name + ":" + v.(string) + ","
		}
	}
	//パラメーターを文字列で格納
	rq.ParamsBasic.ParamsStrings = s

	//ParamsBasicに追加
	for _, v := range rq.PostParameter {
		switch v.Name {
		case "ClientId":
			rq.ParamsBasic.ClientId = v.StringValue
		case "Orders":
			for _, s := range v.StringArray {
				kv := strings.Split(s.Value, ":")
				rq.ParamsBasic.Orders = append(rq.ParamsBasic.Orders, REQ.Order{Name: kv[0], Value: kv[1]})
			}
		case "BqProject":
			rq.ParamsBasic.BqSource.Project = v.StringValue
		case "BqDataset":
			rq.ParamsBasic.BqSource.Dataset = v.StringValue
		case "DepthCutter_Type":
			rq.ParamsBasic.DepthCutter.Type = v.StringValue
		case "DepthCutter_Ope":
			rq.ParamsBasic.DepthCutter.Ope = v.StringValue
		case "DepthCutter_Rate":
			rq.ParamsBasic.DepthCutter.Rate = v.Float32Value
		case "DepthCutter_Level":
			rq.ParamsBasic.DepthCutter.Level = v.IntValue
		case "SearchTagsWordTiedUp":
			rq.ParamsBasic.SearchTagsWordTiedUp = v.BoolValue
		case "MaxItemIndexQty":
			rq.ParamsBasic.MaxItemIndexQty = v.IntValue
		case "StartDate":
			rq.ParamsBasic.StartDate = v.IntValue
		case "EndDate":
			rq.ParamsBasic.EndDate = v.IntValue
		case "LatestRevision":
			rq.ParamsBasic.LatestRevision = v.BoolValue
		case "BqNoInsert":
			rq.ParamsBasic.BqNoInsert = v.BoolValue
		default:
			continue
		}
	}
	return nil
}

///////////////////////////////////////////////////
/* ===========================================
【SerachAPI】【GetAPI】
Attachemnt Search
データセットをQueryする際に送信されてくるPostDataを格納する構造体
=========================================== */
type SearchPostData struct {
	Type          string      `json:"type"`
	ClientId      string      `json:"client_id"`
	CurrentParams string      `json:"current_params"`
	CurrentUrl    string      `json:"current_url"`
	Query         Query       `json:"query"`
	CustomerUuid  string      `json:"customer_uuid"`
	WhatYaId      string      `json:"hmt_id"`
	ChainedTags   ChainedTags `json:"chained_tags"`
	SearchFrom    string      `json:"search_from"`
}
type Query struct {
	ItemId  string   `json:"item_id"`
	Tags    []string `json:"tags"`
	Limit   int      `json:"limit"`
	Orders  []Order  `json:"orders"`
	Filters []Filter `json:"filters"`
	ItemIds []string `json:"item_ids"` //Itemsの検索,SpecailTagItemの取得に利用
	IDKeys  []int64  `json:"id_keys"`  //DatastoreのID keyでの検索に利用
}
type ChainedTags struct {
	ATID            string       `json:"ATID"`
	DDID            int          `json:"DDID"`
	RelatedUnixtime int          `json:"related_unixtime"`
	RelatedWordsLog [][]string   `json:"related_words_log"`
	SelectedItem    SelectedItem `json:"selected_item"`
}
type Order struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}
type Filter struct {
	Name  string `json:"name"`
	Ope   string `json:"ope"`
	Value string `json:"value"`
}
type SelectedItem struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}

///////////////////////////////////////////////////
/* ===========================================
【SerachAPI】【GetAPI】
//SearchAPIリクエストのPOSTパラメーターを取得
=========================================== */
func ParseAttachmentSearchItemsJsonData(r *http.Request, rq *REQ.RequestData) error {

	//parse json request body
	body, length, err := ParseJson(r, rq)
	if err != nil {
		return err
	}

	//parse json
	var jsonBody SearchPostData
	err = json.Unmarshal(body[:length], &jsonBody)
	if err != nil {
		err = errors.New(err.Error())
		return err
	}

	var s string //ParamsStringsの箱

	//reflectを使って構造体の中身を探索しつつ、Parameterを格納
	var p REQ.PostParameter
	rtCst := reflect.TypeOf(jsonBody)
	rvCst := reflect.ValueOf(jsonBody)
	for i := 0; i < rtCst.NumField(); i++ {

		f := rtCst.Field(i) // フィールド情報を取得
		v := rvCst.FieldByName(f.Name).Interface()
		//fmt.Println(f.Tag, f.Name, v)

		switch f.Name {
		case "Query":
			//Tagsのデータを取得
			var tags []REQ.StringValue
			for _, t := range jsonBody.Query.Tags {
				tags = append(tags, REQ.StringValue{Value: t})
				s = s + f.Name + "_Tags:" + t + ","
			}
			p = REQ.PostParameter{Name: "Query_Tags", Type: "StringArray", StringArray: tags}
			rq.PostParameter = append(rq.PostParameter, p)

			//ItemIdのデータを取得
			p = REQ.PostParameter{Name: "Query_ItemId", Type: "StringValue", StringValue: jsonBody.Query.ItemId}
			rq.PostParameter = append(rq.PostParameter, p)
			s = s + f.Name + "_ItemId:" + jsonBody.Query.ItemId + ","

			//limitを設定(base.limitのリクエストがなければ-1に設定)
			limit := jsonBody.Query.Limit
			if limit == 0 {
				limit = -1
			}
			p = REQ.PostParameter{Name: "Query_Limit", Type: "IntValue", IntValue: limit}
			rq.PostParameter = append(rq.PostParameter, p)
			s = s + f.Name + "_Limit:" + strconv.Itoa(limit) + ","

			//Orderを設定
			var orders []REQ.StringValue
			for _, o := range jsonBody.Query.Orders {
				orders = append(orders, REQ.StringValue{Value: o.Name + ":" + o.Value})
				s = s + f.Name + "_Orders:" + o.Name + ":" + o.Value + ","
			}
			p = REQ.PostParameter{Name: "Query_Orders", Type: "StringArray", StringArray: orders}
			rq.PostParameter = append(rq.PostParameter, p)

			//Filtersを設定(平仄をあわせる形で格納、履歴を取得しておく)
			var filters []REQ.StringValue
			for _, f := range jsonBody.Query.Filters {
				filters = append(filters, REQ.StringValue{Value: f.Name + ":" + f.Value})
				s = s + f.Name + "_Filters:" + f.Name + ":" + f.Ope + ":" + f.Value + ","
			}
			p = REQ.PostParameter{Name: "Query_Filters", Type: "StringArray", StringArray: filters}
			rq.PostParameter = append(rq.PostParameter, p)

			//Filterは別途Mapにしておく
			filtersMap := make(map[string]REQ.Filters)
			var flts REQ.Filters
			var flt REQ.Filter
			var itemsFilters REQ.Filters
			for _, f := range jsonBody.Query.Filters {
				var filter = REQ.Filter{Name: f.Name, Ope: f.Ope, Value: f.Value}
				itemsFilters.Filters = append(itemsFilters.Filters, filter)
				flt = REQ.Filter{Name: f.Name, Ope: f.Ope, Value: f.Value}
				flts.Filters = append(flts.Filters, flt)
				flts.Comparison = COMMON.Comparison //※関数をmapにする試験を兼ねて、比較演算関数を入れる
			}
			filtersMap[rq.ParamsBasic.Kind] = flts //Kind名をキーとしてFiltersを格納させる
			rq.Filters = filtersMap

			//ItemIdsのデータを取得
			var itemIds []REQ.StringValue
			for _, id := range jsonBody.Query.ItemIds {
				itemIds = append(itemIds, REQ.StringValue{Value: id})
				s = s + f.Name + "_ItemIds:" + id + ","
			}
			p = REQ.PostParameter{Name: "Query_ItemIds", Type: "StringArray", StringArray: itemIds}
			rq.PostParameter = append(rq.PostParameter, p)

			//IDKeysのデータを取得
			var idKeys []REQ.Int64Value
			for _, idk := range jsonBody.Query.IDKeys {
				idKeys = append(idKeys, REQ.Int64Value{Value: idk})
				s = s + f.Name + "_IDKeys:" + strconv.FormatInt(idk, 10) + ","
			}
			p = REQ.PostParameter{Name: "Query_IDKeys", Type: "IDKeyArray", IDKeyArray: idKeys}
			rq.PostParameter = append(rq.PostParameter, p)

		case "ChainedTags":
			//ATIDのデータを取得
			p = REQ.PostParameter{Name: "ChainedTags_ATID", Type: "StringValue", StringValue: jsonBody.ChainedTags.ATID}
			rq.PostParameter = append(rq.PostParameter, p)
			//DDIDのデータを取得
			p = REQ.PostParameter{Name: "ChainedTags_DDID", Type: "IntValue", IntValue: jsonBody.ChainedTags.DDID}
			rq.PostParameter = append(rq.PostParameter, p)
			//RelatedUnixtimeのデータを取得
			p = REQ.PostParameter{Name: "ChainedTags_RelatedUnixtime", Type: "IntValue", IntValue: jsonBody.ChainedTags.RelatedUnixtime}
			rq.PostParameter = append(rq.PostParameter, p)
			//RelatedWordsLogのデータを取得
			var logs [][]string
			logs = append(logs, jsonBody.ChainedTags.RelatedWordsLog...)
			p = REQ.PostParameter{Name: "ChainedTags_RelatedWordsLog", Type: "StringArrayArray", StringArrayArray: logs}
			rq.PostParameter = append(rq.PostParameter, p)

			//選択されたアイテムのID
			p = REQ.PostParameter{Name: "ChainedTags_SelectedItem_Id", Type: "StringValue", StringValue: jsonBody.ChainedTags.SelectedItem.Id}
			rq.PostParameter = append(rq.PostParameter, p)

			//選択されたアイテムのName
			p = REQ.PostParameter{Name: "ChainedTags_SelectedItem_Name", Type: "StringValue", StringValue: jsonBody.ChainedTags.SelectedItem.Name}
			rq.PostParameter = append(rq.PostParameter, p)

		default:
			p = REQ.PostParameter{Name: f.Name, Type: "StringValue", StringValue: v.(string)}
			rq.PostParameter = append(rq.PostParameter, p)
			s = s + f.Name + ":" + v.(string) + ","
		}
	}

	//パラメーターを文字列で格納
	rq.ParamsBasic.ParamsStrings = s

	//ParamsBasicに追加
	for i, v := range rq.PostParameter {
		switch v.Name {
		case "ClientId":
			rq.ParamsBasic.ClientId = v.StringValue
		case "CurrentUrl":
			rq.ParamsBasic.CurrentUrl = v.StringValue
		case "CurrentParams":
			v.StringValue = strings.Replace(v.StringValue, "?", "", -1)
			arr := strings.Split(v.StringValue, "&")
			names := getTriggerNames() //トリガー名を取得する
			for _, a := range arr {
				if a != "" {
					kv := strings.Split(a, "=")
					if len(kv) != 2 {
						break
					}

					//パラメーター名の置き換え
					var kn string
					if COMMON.StringSliceSearch(names.itemIds, kv[0]) {
						kn = "ItemId"
					} else {
						kn = kv[0]
					}
					var p REQ.GetParameter = REQ.GetParameter{Name: kn, Value: kv[1]}
					rq.ParamsBasic.CurrentParams = append(rq.ParamsBasic.CurrentParams, p)
				}
			}
		case "Query_Tags":
			//【タグが空の配列だった場合】
			//※検索される順番必須　CurrentParams > CurrentUrl > Query_Tags
			//*上記の理由から、パラメーターの取得順番もClientId,CurrentParams,CurrentUrl,Query_Tagsを崩してはならない
			//1.CurrentParamsにItemIdが指定されていれば、これを採用
			//2.上記以外、フォーマットにしたがって、CurrentURLから抽出
			//[TAG検索優先度]https://svc.atlassian.net/wiki/spaces/CDLAB/pages/1149337662/Ph3+4+Attachment+Tugcar
			if v.StringArray == nil {
				switch rq.ParamsBasic.ClientId {
				default:
					itemId := getItemIdTagFromCurrentParamsOrUrl(rq)
					var sv REQ.StringValue = REQ.StringValue{Value: itemId}
					rq.PostParameter[i].StringArray = append(rq.PostParameter[i].StringArray, sv)
				}
			}
		case "Query_Orders":
			for _, o := range jsonBody.Query.Orders {
				var odr REQ.Order
				odr.Name = o.Name
				odr.Value = o.Value
				rq.ParamsBasic.Orders = append(rq.ParamsBasic.Orders, odr)
			}
		default:
			continue
		}
	}
	return nil
}

///////////////////////////////////////////////////
/* ===========================================
【SerachAPI】【GetAPI】
CurrentParamsまたはCurrentUrlからItemIdを抽出して返却
=========================================== */
func getItemIdTagFromCurrentParamsOrUrl(rq *REQ.RequestData) string {

	client := rq.ParamsBasic.ClientId
	params := rq.ParamsBasic.CurrentParams
	url := rq.ParamsBasic.CurrentUrl

	//ParamsにItemIdの指定があるか検索、見つかれば返却
	for _, p := range params {
		if p.Name == "ItemId" {
			return p.Value
		}
	}

	//正規表現で文字列を抽出
	var regEx string
	switch client {
	default:
		regEx = `[\d]+`
	}
	r := regexp.MustCompile(regEx)
	str := r.FindAllStringSubmatch(url, -1)

	//抽出した文字列配列から該当の要素を指定(基本的に[0][?]に欲しいもの入るようにregExを調整する)
	var index int = 0
	switch client {
	default:
		//index = 0
	}

	//[0][?]をItemIdとして返却
	var rs string
	if len(str) > 0 {
		rs = string(str[0][index])
	}
	return rs
}

///////////////////////////////////////////////////
/* ===========================================
【DeleteAPI】
Attachemnt Delete
データセットをDeleteする際に送信されてくるPostDataを格納する構造体
=========================================== */
type DeletePostData struct {
	Type           string `json:"type"`
	ClientId       string `json:"client_id"`
	Ope            string `json:"ope"`
	Revision       int    `json:"revision"`
	LatestRevision string `json:"latest_revision"`
}

func ParseAttachmentDeleteJsonData(r *http.Request, rq *REQ.RequestData) error {

	//parse json request body
	body, length, err := ParseJson(r, rq)
	if err != nil {
		return err
	}

	//parse json
	var jsonBody DeletePostData
	err = json.Unmarshal(body[:length], &jsonBody)
	if err != nil {
		err = errors.New(err.Error())
		return err
	}

	var s string //ParamsStringsの箱

	//reflectを使って構造体の中身を探索しつつ、Parameterを格納
	var p REQ.PostParameter
	rtCst := reflect.TypeOf(jsonBody)
	rvCst := reflect.ValueOf(jsonBody)
	for i := 0; i < rtCst.NumField(); i++ {
		f := rtCst.Field(i) // フィールド情報を取得
		v := rvCst.FieldByName(f.Name).Interface()
		//fmt.Println(f.Tag, f.Name, v)
		switch f.Name {
		case "Revision":
			p = REQ.PostParameter{Name: f.Name, Type: "IntValue", IntValue: v.(int)}
			rq.PostParameter = append(rq.PostParameter, p)
			s = s + f.Name + ":" + strconv.Itoa(v.(int)) + ","
		case "LatestRevision":
			p = REQ.PostParameter{Name: f.Name, Type: "StringValue", StringValue: v.(string)} //指定有無判定のため、文字列で指定
			rq.PostParameter = append(rq.PostParameter, p)
			s = s + f.Name + ":" + v.(string) + ","
		default:
			p = REQ.PostParameter{Name: f.Name, Type: "StringValue", StringValue: v.(string)}
			rq.PostParameter = append(rq.PostParameter, p)
			s = s + f.Name + ":" + v.(string) + ","
		}
	}

	//パラメーターを文字列で格納
	rq.ParamsBasic.ParamsStrings = s

	//ParamsBasicに追加
	for _, v := range rq.PostParameter {
		switch v.Name {
		case "ClientId":
			rq.ParamsBasic.ClientId = v.StringValue
		default:
			continue
		}
	}
	return nil
}
