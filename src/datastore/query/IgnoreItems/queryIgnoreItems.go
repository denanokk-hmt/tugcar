/*======================
Datastoreの
Namespace: WhatYa-Attachment-[client]-[env]
kind: IgnoreItems
に関するクエリ
========================*/
package query

import (
	ENTITY "bwing.app/src/datastore/entity"
	QUERY "bwing.app/src/datastore/query"
	REQ "bwing.app/src/http/request"
	"cloud.google.com/go/datastore"
)

///////////////////////////////////////////////////
//Set Transaction
func NewTransaction(run bool) {
	//tran :=
}

//Interface
type QueryIgnoreItems struct {
	Queries QUERY.Queries
}

///////////////////////////////////////////////////
/* ===========================================
IgnoreItemsを検索
sync.Mapを使えない=固有キーの無い検索
* =========================================== */
func (qig *QueryIgnoreItems) GetIgnoreItems(rq *REQ.RequestData, client *datastore.Client) (interface{}, []*datastore.Key, error) {

	//Filterを設定する箱を準備（連想配列名にフィルター名、要素に値）
	args := make(map[string]string)

	//Filterに必要な、Prop名&オペ、値(文字列)を設定
	args["filter_IgnoreDflg_beq"] = "false"

	//Get parameterとしてFilterを設定(Get parameterを初期化)
	REQ.SettingFilterToGetParamter(rq, &args, true)

	//IgnoreItemsを取得
	e, pkeys, err := qig.Queries.GetAllByFilter(rq, client, ENTITY.KIND_IGNOREITEMS, nil)
	if err != nil {
		return nil, nil, err
	}

	return e, pkeys, nil
}
