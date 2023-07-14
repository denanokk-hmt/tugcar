/*
======================
リクエスト関連の処理の共通処理
========================
*/
package request

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	CONFIG "bwing.app/src/config"
	ENTITY "bwing.app/src/datastore/entity"
)

var (
	ACTION_PATH_POSITION     = 3
	SECONDARY_PATH_POSITION  = 4
	ACTION_PATH_LOAD         = "load"
	ACTION_PATH_UPDATE       = "update"
	ACTION_PATH_SEARCH       = "search"
	ACTION_PATH_GET          = "get"
	MAP_ACTION_STORE         = "Store"
	MAP_ACTION_LOAD_OR_STORE = "LoadOrStore"
	MAP_ACTION_DELETE        = "Delete"
	MAP_ACTION_RANGE         = "Range"
)

// Request interface
type Requests interface {
	//Datastore向けのリクエスト
	GetDsSwitch(w http.ResponseWriter, r *http.Request)
	PostDsWithJsonSwitch(w http.ResponseWriter, r *http.Request)
	PostDsSwitch(w http.ResponseWriter, r *http.Request)
	PutDsSwitch(w http.ResponseWriter, r *http.Request)
	DeleteDsSwitch(w http.ResponseWriter, r *http.Request)
}

// Request interface
type Requests2 interface {
	//BigQuery向けのリクエスト
	PostBqWithJsonSwitch(w http.ResponseWriter, r *http.Request)
}

// httpリクエスト情報を格納する箱
type RequestData struct {
	Method              string
	Host                string
	Urlpath             string
	UrlPathArry         []string
	Uri                 string
	Header              interface{}
	GetParameter        []GetParameter
	GetParameterMap     map[string][]GetParameter
	GetParameterSyncMap sync.Map
	PostParameter       []PostParameter
	ParamsBasic         ParamsBasic
	Transaction         bool
	Filters             map[string]Filters
}

// APIに応じた基本パラメーター
type ParamsBasic struct {
	Action               string
	ClientId             string
	Token                string
	Kind                 string
	Seconddary           string
	LastPath             string
	BqSource             BigQuery   //LoadAPIに利用
	DepthCutter          DepthValue //LoadAPI(ItemIndex)に利用
	SearchTagsWordTiedUp bool       //LoadAPI(ItemIndex)に利用
	MaxItemIndexQty      int        //LoadAPI(ItemIndex)に利用
	StartDate            int
	EndDate              int
	Orders               []Order
	CurrentUrl           string
	CurrentParams        []GetParameter
	Cdt                  time.Time
	ParamsStrings        string
	LatestRevision       bool //Load時にLatestRevisionのUpdateを行うかどうか
	BqNoInsert           bool
}
type BigQuery struct {
	Project string
	Dataset string
}

type DepthValue struct {
	Type  string
	Ope   string
	Rate  float32
	Level int
}

type Order struct {
	Name  string
	Value string //"auto" or "asc" or "desc"
}

// Search API Filter Parameter
type Filters struct {
	Filters    []Filter
	Comparison func(string, string, string) bool
}
type Filter struct {
	Name  string
	Ope   string
	Value string
}

// GETパラメーター情報を格納する箱
type GetParameter struct {
	Name  string
	Value string
}

// POSTパラメーター情報を格納する箱
type PostParameter struct {
	Name             string
	Type             string
	StringValue      string
	IntValue         int
	Int32Value       int32
	Int64Value       int64
	Float32Value     float32
	Float64Value     float64
	BoolValue        bool
	TimeValue        time.Time
	StringArray      []StringValue //文字配列 ["A", "B"]
	StringArrayArray [][]string
	//Int32Array   []int32
	//IntArray     []int //整数配列 [1, 2]
	//Int64Array []int64
	//Float32Array []float32
	//Float64Array []float64
	IDKeyValue   int64        //datastoreのIDKeyの値
	IDKeyArray   []Int64Value //datastoreのIDKeyのint64配列
	NameKeyValue string       //datastoreのNameKeyの値
}

// Array向け//Words(tag)向け等
type StringValue struct {
	Value string
}

// Array向け//__Key__(id)向け等
type Int64Value struct {
	Value int64
}

///////////////////////////////////////////////////
/* ===========================================
Request情報を取得
* =========================================== */
func NewRequestData(r *http.Request) RequestData {
	var rq RequestData = RequestData{
		Method:      r.Method,
		Host:        r.Host,
		Urlpath:     r.URL.Path,
		UrlPathArry: strings.Split(r.URL.Path, "/"),
		Uri:         r.RequestURI,
		Header:      r.Header}
	return rq
}

///////////////////////////////////////////////////
/* ===========================================
Get ACTION名を格納する
※前提::Pathの前方から3つ目(はがAction名である
* =========================================== */
func NewActionName(r *http.Request, rq *RequestData) {
	//(UrlPathの前から3番目:/hmt/attachment/[Action]/)
	p := rq.UrlPathArry
	a := p[ACTION_PATH_POSITION]
	rq.ParamsBasic.Action = a
}

///////////////////////////////////////////////////
/* ===========================================
Get ACTION名の次を格納する
※前提::Pathの前方から3つ目(はがAction名である
* =========================================== */
func NewSecondaryName(r *http.Request, rq *RequestData) {
	//(UrlPathの前から4番目:/hmt/attachment/[Action]/[2ndary]/)
	p := rq.UrlPathArry
	a := p[SECONDARY_PATH_POSITION]
	rq.ParamsBasic.Seconddary = a
}

///////////////////////////////////////////////////
/* ===========================================
Get KIND名を格納する
※前提::Pathの末尾がkind名である
* =========================================== */
func NewKindName(r *http.Request, rq *RequestData) {
	//(UrlPathの後ろから1番目:/hmt/attachment/[Action]/[kind])
	//URLPathからDSのKind名を取得して判定(登録済みのkindかどうかチェック)
	k, b := ENTITY.GetKindName(rq.UrlPathArry)
	if !b {
		rq.Urlpath = k + rq.Urlpath //以下の処理で例外へ飛ぶようにパスを変更
	}
	rq.ParamsBasic.Kind = k
}

// /////////////////////////////////////////////////
// Get Last name
func NewLastName(r *http.Request, rq *RequestData) {
	//(Urlの後ろから1番目:/hmt/attachment/[Action]/[last])
	//Get action name from url path (exp: /hmt/attachment/[action]/[last])
	p := rq.UrlPathArry
	lp := len(p)
	l := p[lp-1]
	rq.ParamsBasic.LastPath = l
}

///////////////////////////////////////////////////
/* ===========================================
Get クエリパラメーターを格納する
* =========================================== */
func NewParseForm(r *http.Request, rq *RequestData) error {

	if err := r.ParseForm(); err != nil {
		fmt.Println(err)
		return err
	}
	for k, v := range r.Form {
		gp := GetParameter{Name: k, Value: v[0]}
		rq.GetParameter = append(rq.GetParameter, gp)
	}
	return nil
}

///////////////////////////////////////////////////
/* ===========================================
Post formDataを取得
-H "Content-type: multipart/form-data"
* =========================================== */
func NewParseMultiForm(r *http.Request, rq *RequestData) {

	if err := r.ParseMultipartForm(32 << 20); err != nil {
		fmt.Println("errorだよ")
	}
	for k, v := range r.Form {
		//pp := PostParameter{Name: k, Value: v[0]}
		//rq.PostParameter = append(rq.PostParameter, pp)
		fmt.Println(k, v)
	}
}

///////////////////////////////////////////////////
/* ===========================================
Post parameterの中でKeyを指定しているデータに対して
型をDatastoreに合わせて変換する
※共通：個別対応なし
* =========================================== */
func ConvertTypeKeyParameter(rq *RequestData) {
	//DatastoreのPropertyに合わせてCast
	for n, v := range rq.PostParameter {
		switch v.Name {
		case "IDKey":
			switch v.Type {
			case "string":
				ov, _ := strconv.ParseInt(rq.PostParameter[n].StringValue, 10, 64)
				rq.PostParameter[n].StringValue = ""
				rq.PostParameter[n].IDKeyValue = ov
			case "int":
				ov := rq.PostParameter[n].IntValue
				rq.PostParameter[n].IntValue = 0
				rq.PostParameter[n].IDKeyValue = int64(ov)
			case "float32":
				ov := rq.PostParameter[n].Float32Value
				rq.PostParameter[n].Float32Value = 0
				rq.PostParameter[n].IDKeyValue = int64(ov)
			case "float64":
				ov := rq.PostParameter[n].Float64Value
				rq.PostParameter[n].Float64Value = 0
				rq.PostParameter[n].IDKeyValue = int64(ov)
			default:
			}
			rq.PostParameter[n].Type = "IDKey"
		case "NameKey":
			ov := rq.PostParameter[n].StringValue
			rq.PostParameter[n].StringValue = ""
			rq.PostParameter[n].NameKeyValue = ov
		default:
			//fmt.Println(v)
		}
		//fmt.Println(n, v)
	}
}

///////////////////////////////////////////////////
/* ===========================================
Post parameterの型をDatastoreに合わせて変換する
※個別にproperty別で追加していく
* =========================================== */
func ConvertTypeParams(rq *RequestData) {

	var propPublishedAt string

	//DatastoreのPropertyに合わせてCast
	//*Article kindの場合、Number以外は、文字列で入ってくるので指定なし
	for n, v := range rq.PostParameter {
		switch v.Name {
		case "Number": //Number propertyをint型にする
			switch v.Type {
			case "string":
				ov, _ := strconv.Atoi(rq.PostParameter[n].StringValue)
				rq.PostParameter[n].StringValue = ""
				rq.PostParameter[n].IntValue = ov
			case "float32":
				ov := rq.PostParameter[n].Float32Value
				rq.PostParameter[n].Float32Value = 0
				rq.PostParameter[n].IntValue = int(ov)
			case "float64":
				ov := rq.PostParameter[n].Float64Value
				rq.PostParameter[n].Float64Value = 0
				rq.PostParameter[n].IntValue = int(ov)
			default:
			}
			rq.PostParameter[n].Type = "int"
		case "PublishedAt":
			propPublishedAt = v.Name
			ov := rq.PostParameter[n].TimeValue
			rq.PostParameter[n].StringValue = ""
			rq.PostParameter[n].TimeValue = ov
		default:
			//fmt.Println(v)
		}
	}

	//PublishedAtの指定がない→現在時刻
	if propPublishedAt == "" {
		/*
			t := time.Now()
			fmt.Println(t)           // => "2015-05-05 07:23:30.757800829 +0900 JST"
			fmt.Println(t.Year())    // => "2015"
			fmt.Println(t.Month())   // => "May"
			fmt.Println(t.Day())     // => "5"
			fmt.Println(t.Hour())    // => "7"
			fmt.Println(t.Minute())  // => "23"
			fmt.Println(t.Second())  // => "30"
			fmt.Println(t.Weekday()) // => "Tuesday"
		*/
		p := PostParameter{Name: "PublishedAt", Type: "time.Time", TimeValue: time.Now()}
		//fmt.Println(p.TimeValue)
		rq.PostParameter = append(rq.PostParameter, p)
	}
}

///////////////////////////////////////////////////
/* ===========================================
Get parameterとしてFilterを設定
	map型で、連想配列名、要素名を値に指定（どちらもstring型）
	"filter_" & prop名 & "_" & ope 例:filter_LatestRevision_beq
	※先に指定しているGetParamterを消す場合、引数のinitをtrue
* =========================================== */
func SettingFilterToGetParamter(rq *RequestData, args *map[string]string, init bool) {

	//Get parameterを初期化
	if init {
		rq.GetParameter = rq.GetParameter[:0]
	}

	//Get parameterとしてFilterを設定
	for k, v := range *args {
		rq.GetParameter = append(rq.GetParameter, GetParameter{Name: k, Value: v})
	}
}

///////////////////////////////////////////////////
/* ===========================================
Get parameterとしてFilterを設定した文字列のMapをsync.Mapに格納
action:
	Store:値の格納、LoadOrStore:値の格納と読み込み、Delete:値の削除、Range:値の出力
args:
	Store、LoadStoreの場合に必要
	Map型で、連想配列名、要素名を値に指定（どちらもstring型）
	"filter_" & prop名 & "_" & ope 例:filter_LatestRevision_beq
* =========================================== */
func SettingFilterToGetParamterSyncMap(rq *RequestData, action string, args *map[string]string, mapKey string) {

	switch action {
	case MAP_ACTION_STORE, MAP_ACTION_LOAD_OR_STORE: //GetParameterSyncMapsに格納
		var gps []GetParameter
		for k, v := range *args {
			var gp GetParameter = GetParameter{Name: k, Value: v}
			gps = append(gps, gp) //GetParameterSyncMapに登録するKey-Valueを格納
		}
		if len(gps) == 0 {
			return
		}
		if action == MAP_ACTION_STORE {
			rq.GetParameterSyncMap.Store(mapKey, gps)
		} else {
			load, ok := rq.GetParameterSyncMap.LoadOrStore(mapKey, gps)
			if ok {
				fmt.Printf("Key: %v -> Load: %v\n", mapKey, load)
			}
		}
	case MAP_ACTION_DELETE: //GetParameterSyncMapsから削除
		rq.GetParameterSyncMap.Delete(mapKey)
	case MAP_ACTION_RANGE: ////GetParameterSyncMapsを出力
		rq.GetParameterSyncMap.Range(func(key interface{}, value interface{}) bool {
			fmt.Printf("Key: %v -> Value: %v\n", key, value)
			return true
		})
	}
}

///////////////////////////////////////////////////
/* ===========================================
Token認証を行う
	1.headerにTokenを指定した場合
		Authorization: Bearer <token>
	2.POST dataにTokenを指定した場合
		auth_token: <token>
	1. or 2. どちらか一方にtokenを指定
* =========================================== */
func AuthorizationToken(r *http.Request, rq *RequestData) bool {

	var token string
	token = r.Header.Get("Authorization")
	if token == "" {
		if rq.Method == "Get" {
			for _, v := range rq.GetParameter {
				fmt.Println(v)
				if v.Name == "auth_token" {
					token = v.Value
				}
			}
		} else {
			for _, v := range rq.PostParameter {
				fmt.Println(v)
				if v.Name == "AuthToken" {
					token = v.StringValue
				}
			}
		}
	}

	if token == "" {
		return false
	}

	for _, t := range CONFIG.GetUuv4Tokens() {
		if t == token {
			return true
		}
	}

	return false
}
