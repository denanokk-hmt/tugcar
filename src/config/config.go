/* =================================
サーバーのConfigを設定する
* ================================= */
package config

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"runtime"
)

var (
	PROJECT_ID  = "ProjectId"
	SERVER_CODE = "ServerCode"
	APPLI_NAME  = "AppliName"
	ENV         = "Env"
	SERIES      = "Series"
)

/*
JSONファイルからのサーバーコンフィグ値の箱
type Config struct {
	ProjectId string `json:"project_id"`
	ServerCode string `json:"server_code"`
	AppliName  string `json:"appli_name"`
	Env        string `json:"env"`
	Series     string `json:"series"`
}*/
var configMap map[string]string //サーバーコンフィグ値の箱
var uuv4Tokens []string         //サーバー認証のためのTokenの箱

///////////////////////////////////////////////////
//起動時にGCP ProjectID、NS, Kindを登録する
func init() {
	NewConfig() //Set server basic config values
	fmt.Printf("Tugcar Start On [Project:%s][ServerCode:%s][Appli:%s][Env:%s][Series:%s]",
		configMap[PROJECT_ID],
		configMap[SERVER_CODE],
		configMap[APPLI_NAME],
		configMap[ENV],
		configMap[SERIES])
}

///////////////////////////////////////////////////
/* =================================
	Docker CMD Args
	※CMDの第一引数は、実行ファイルパスだが、flag.Parse()は、
	実行ファイルパスの次の引数から始まる
	//$PORT						::args[0]
	//$GCP_PROJECT_ID	::args[1]
	//$SERVER_CODE		::args[2]
	//$APPLI_NAME			::args[3]
	//$ENV						::args[4]
	//$SERIES					::args[5]
* ================================= */
func NewConfig() {

	//起動時の引数から取得
	flag.Parse()
	args := flag.Args()

	//コンフィグをMapping
	configMap = make(map[string]string)
	configMap[PROJECT_ID] = args[1]
	configMap[SERVER_CODE] = args[2]
	configMap[APPLI_NAME] = args[3]
	configMap[ENV] = args[4]
	configMap[SERIES] = args[5]
}

///////////////////////////////////////////////////
/* =================================
	//Configの返却
* ================================= */
func GetConfig(name string) string {
	return configMap[name]
}
func GetConfigAll() map[string]string {
	return configMap
}

///////////////////////////////////////////////////
/* =================================
サーバー間認証に用いるUUV4トークンをJSONファイルから取得しておく
* ================================= */
func NewUuv4Tokens() {

	//箱を準備
	type Uuv4TokenJson struct {
		Uuv4tokens []string `json:"uuv4tokens"`
	}

	//Rootディレクトリを取得して、tokensのJSONファイルの絶対パスを指定
	var (
		_, b, _, _ = runtime.Caller(0)
		root       = filepath.Join(filepath.Dir(b), "../../")
	)
	path := root + "/cmd/authorization/uuv4tokens.json"

	// JSONファイル読み込み
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}

	// JSONデコード
	var tokens Uuv4TokenJson
	if err := json.Unmarshal(bytes, &tokens); err != nil {
		log.Fatal(err)
	}
	// デコードしたデータを表示
	for _, t := range tokens.Uuv4tokens {
		uuv4Tokens = append(uuv4Tokens, t)
	}
}
func GetUuv4Tokens() []string {
	return uuv4Tokens
}
