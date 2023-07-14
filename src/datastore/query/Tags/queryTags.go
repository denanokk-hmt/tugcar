/*
======================
Datastoreの
Namespace: WhatYa-Attachment-[client]-[env]
kind: Tags
に関するクエリ
========================
*/
package query

import (
	"fmt"
	"strconv"

	ENTITY "bwing.app/src/datastore/entity"
	QUERY "bwing.app/src/datastore/query"
	REQ "bwing.app/src/http/request"
	"cloud.google.com/go/datastore"
)

// /////////////////////////////////////////////////
// Set Transaction
func NewTransaction(run bool) {
	//tran :=
}

// Interface
type QueryTags struct {
	Queries QUERY.Queries
}

///////////////////////////////////////////////////
/* ===========================================
TagsWordでItemIdを検索
* =========================================== */
func (qt *QueryTags) GetItemIdByTagsWord(rq *REQ.RequestData, tag string, client *datastore.Client) (interface{}, error) {

	//Filterを設定する箱を準備（連想配列名にフィルター名、要素に値）
	args := make(map[string]string)

	//Filterに必要な、Prop名&オペ、値(文字列)を設定
	args["order_ItemId"] = "asc"
	args["filter_LatestRevision_beq"] = "true"
	args["filter_TagsWord_eq"] = tag

	//Get parameterをSync Map(Filterを設定)
	REQ.SettingFilterToGetParamterSyncMap(rq, REQ.MAP_ACTION_STORE, &args, tag)

	//TagsWordに紐づくItemIdを取得
	entity, _, err := qt.Queries.GetAllByFilterSyncMap(rq, client, ENTITY.KIND_TAGS, nil, tag)
	if err != nil {
		return nil, err
	}

	return entity, nil
}

///////////////////////////////////////////////////
/* ===========================================
親キー(Items:ID)、またはItemIdでTagsWordを検索
* =========================================== */
func (qt *QueryTags) GetTagsWordByItemId(rq *REQ.RequestData, itemId string, pKeys *datastore.Key, client *datastore.Client) (interface{}, error) {

	//PKeyでの検索ではない場合、
	if pKeys == nil {
		//Filterを設定する箱を準備（連想配列名にフィルター名、要素に値）
		args := make(map[string]string)

		//Filterに必要な、Prop名&オペ、値(文字列)を設定
		args["filter_LatestRevision_beq"] = "true"
		args["filter_ItemId_eq"] = itemId

		//Get parameterをSync Map(Filterを設定)
		REQ.SettingFilterToGetParamterSyncMap(rq, REQ.MAP_ACTION_STORE, &args, itemId)
	}

	//TagsWordに紐づくItemIdを取得
	entity, _, err := qt.Queries.GetAllByFilterSyncMap(rq, client, ENTITY.KIND_TAGS, pKeys, itemId)
	if err != nil {
		return nil, err
	}

	return entity, nil
}

///////////////////////////////////////////////////
/* ===========================================
最新LoadしたEntityより古く、LatestRevisionがTRUEであるEntityの
LatestRevisionをFALSEに更新する
* =========================================== */
func (qt *QueryTags) UpdateLatestRevision(rq *REQ.RequestData, client *datastore.Client) error {

	dsKind := rq.ParamsBasic.Kind //Tags

	//unixtimeを算出
	unixtime := rq.ParamsBasic.Cdt.Unix()

	//Filterを設定する箱を準備（連想配列名にフィルター名、要素に値）
	args := make(map[string]string)

	//Filterに必要な、Prop名&オペ、値(文字列)を設定
	args["filter_LatestRevision_beq"] = "true"
	args["filter_Revision_lt"] = strconv.Itoa(int(unixtime))

	//Get parameterとしてFilterを設定(Get parameterを初期化)
	REQ.SettingFilterToGetParamter(rq, &args, true)

	//最新のRevisionより小さいRevision、且つlatestRevisionがtrueのEntityを取得する
	ens, pkeys, err := qt.Queries.GetAllByFilter(rq, client, dsKind, nil)
	if err != nil {
		fmt.Println(err)
		return err
	}
	ens1, _ := ens.(*[]ENTITY.EntityTags) //取得したEntityをCast

	//CdtとLatestRevisonをFALSEに上書き
	for i, _ := range *ens1 {
		(*ens1)[i].Udt = rq.ParamsBasic.Cdt
		(*ens1)[i].LatestRevision = false
	}

	//古いRevのLatestRevisionをFalseに、ChunkしながらUpdate(500件未満づつの更新)
	eis, chunks := ENTITY.CreateChunkBox(dsKind, &ens1)
	for i, c := range chunks.Positions {
		result, err := qt.Queries.PutMultiUsingKey(rq, pkeys[c.Start:c.End], pkeys[c.Start:c.End], eis[i], c.Qty, client, dsKind)
		if err != nil {
			fmt.Println(err, result)
			return err
		}
		fmt.Println("Updated LatestRevision values of old entities to FALSE", c.Start, c.End, dsKind)
	}

	return nil
}

///////////////////////////////////////////////////
/* ===========================================
Revision指定されたEntityのLatestRevisionをTRUEに、
それ以外はFALSEに更新する
* =========================================== */
func (qt *QueryTags) UpdateLatestRevisionByRevision(rq *REQ.RequestData, client *datastore.Client, filters []REQ.Filter, latestRev bool) (int, error) {

	dsKind := rq.ParamsBasic.Kind //Tags

	//Filterを設定する箱を準備（連想配列名にフィルター名、要素に値）
	args := make(map[string]string)

	for _, f := range filters {
		args["filter_"+f.Name+"_"+f.Ope] = f.Value
	}

	//Get parameterとしてFilterを設定(Get parameterを初期化)
	REQ.SettingFilterToGetParamter(rq, &args, true)

	//最新のRevisionより小さいRevision、且つlatestRevisionがtrueのEntityを取得する
	ens, pkeys, err := qt.Queries.GetAllByFilter(rq, client, dsKind, nil)
	if err != nil {
		fmt.Println(err)
		return 0, err
	}
	ens1, _ := ens.(*[]ENTITY.EntityTags) //取得したEntityをCast

	//CdtとLatestRevisonをFALSEに上書き
	for i, _ := range *ens1 {
		(*ens1)[i].Udt = rq.ParamsBasic.Cdt
		(*ens1)[i].LatestRevision = latestRev
	}

	//Chunk数調査
	eis, chunks := ENTITY.CreateChunkBox(dsKind, &ens1)

	//ChunkしながらUpdate(500件未満づつの更新)
	for i, c := range chunks.Positions {
		result, err := qt.Queries.PutMultiUsingKey(rq, pkeys[c.Start:c.End], pkeys[c.Start:c.End], eis[i], c.Qty, client, dsKind)
		if err != nil {
			fmt.Println(err, result)
			return 0, err
		}
		fmt.Println("Updated LatestRevision values of entities to "+strconv.FormatBool(latestRev), c.Start, c.End, dsKind)
	}

	return len(*ens1), nil
}
