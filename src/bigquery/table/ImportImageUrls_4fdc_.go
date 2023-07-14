/*
	=================================

BigQuery
Dataset::attachment_fdc
Table::import_image_urls
テーブルの構造をここで指定する
* =================================
*/
package table

import (
	"time"
)

///////////////////////////////////////////////////
/* ===========================================
Table::
* =========================================== */

var (
	DATASET_ATTACHMENT_FDC  = "attachment_fdc"    //dataset
	TABLE_IMPORT_IMAGE_URLS = "import_image_urls" //tableId
)

// テーブル定義
type ImportImageUrls struct {
	ItemId        string    `bigquery:"item_id"`
	ImageMainUrls []string  `bigquery:"image_main_urls"`
	ImageSubUrls  []string  `bigquery:"image_sub_urls"`
	Cdt           time.Time `bigquery:"cdt"`
}
