/*======================

JsonDataを処理
========================*/
///////////////////////////////////////////////////
/* ===========================================
トリガー名たちを取得
ARGS
	slice: スライス
	value: 検索値
RETURN
	検索結果(あり:true/なし:false)
=========================================== */
package postdata

var itemIdNames = []string{
	"item_id",
	"product_id",
}

var tagNames = []string{
	"tag",
	"word",
	"keyword",
}

//Attachmentのトリガーパラメーター名
type triggerNames struct {
	itemIds  []string
	tagNames []string
}

//ItemIdのParameter名を返却
func getTriggerNames() triggerNames {
	var trigger triggerNames
	trigger.itemIds = itemIdNames
	trigger.tagNames = tagNames
	return trigger
}
