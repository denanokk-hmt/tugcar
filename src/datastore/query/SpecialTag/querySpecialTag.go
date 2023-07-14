/*======================
Datastoreの
Namespace: WhatYa-Attachment-[client]-[env]
kind: SpecialTagItems
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
type QuerySpecialTag struct {
	Queries QUERY.Queries
}

///////////////////////////////////////////////////
/* ===========================================
SpecailTagをTagIDで取得する
* =========================================== */
func (qst *QuerySpecialTag) GetSpecialTagByTagID(rq *REQ.RequestData, client *datastore.Client, tagID string) (interface{}, []*datastore.Key, error) {

	//Filterを設定する箱を準備（連想配列名にフィルター名、要素に値）
	args := make(map[string]string)

	//Filterに必要な、Prop名&オペ、値(文字列)を設定(latestRevisionがtrue、且つ、itemIdと一致)
	args["filter_Dflg_beq"] = "false"
	args["filter_TagID_eq"] = tagID

	//Get parameterとしてFilterを設定
	REQ.SettingFilterToGetParamterSyncMap(rq, REQ.MAP_ACTION_STORE, &args, tagID)

	//SpecialTagをTagIDで検索
	e, keys, err := qst.Queries.GetAllByFilterSyncMap(rq, client, ENTITY.KIND_SPECIALTAG, nil, tagID)
	if err != nil {
		return nil, nil, err
	}

	return e, keys, nil
}
