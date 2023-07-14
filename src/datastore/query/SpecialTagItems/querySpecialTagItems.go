/*======================
Datastoreの
Namespace: WhatYa-Attachment-[client]-[env]
kind: SpecialTagItems
に関するクエリ
========================*/
package query

import (
	"strconv"

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
type QuerySpecialTagItems struct {
	Queries QUERY.Queries
}

///////////////////////////////////////////////////
/* ===========================================
SpecialTagItemsをPKeyと__KEY__(=id)で取得する
* =========================================== */
func (qsti *QuerySpecialTagItems) GetSpecialTagItemsByPKeyIDKey(rq *REQ.RequestData, client *datastore.Client, gr ENTITY.SpecialTagItemsGetRequest) (interface{}, error) {

	//Filterを設定する箱を準備（連想配列名にフィルター名、要素に値）
	args := make(map[string]string)

	//Filterに必要な、Prop名&オペ、値(文字列)を設定
	sIdKey := strconv.FormatInt(gr.IDKey, 10)
	args["key_id"] = sIdKey

	//Get parameterとしてFilterを設定
	REQ.SettingFilterToGetParamterSyncMap(rq, REQ.MAP_ACTION_STORE, &args, sIdKey)

	//NPropsを配列ではないStructを指定させたい。client.Getを利用するため
	dsKind := ENTITY.KIND_SPECIALTAGITEMS + ENTITY.KIND_OPTIONAL_SUFFIX_NOT_ARRAY

	//SpecialTagItemsをParentKeyと__KEY__(id)で検索
	e, err := qsti.Queries.GetByKeySyncMap(rq, client, dsKind, gr.PKey, sIdKey)
	if err != nil {
		return nil, err
	}

	return e, nil
}
