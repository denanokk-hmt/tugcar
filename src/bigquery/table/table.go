/*
	=================================

共有BigQueryのTableの構造
その他、Tableを扱う共通項目や処理
* =================================
*/
package table

import (
	//共通処理
	"time"

	CONFIG "bwing.app/src/config"
)

// Datastore Load result
type LoadDataResults struct {
	Results interface{}
}

// BQデータロード結果
type BqLoadResults struct {
	Result  int
	Client  string
	Cdt     time.Time
	LogNo   int
	LogPath string
	LogDate string
	TTL     int
}

/*
	=================================

//Get GCP ProjectID
* =================================
*/
func GetProjectId() string {
	return CONFIG.GetConfig(CONFIG.PROJECT_ID)
}

/*
	=================================

//結果箱の空の中身
* =================================
*/
func NewLoadDataResults(p interface{}) LoadDataResults {
	var r LoadDataResults
	r.Results = p
	return r
}
