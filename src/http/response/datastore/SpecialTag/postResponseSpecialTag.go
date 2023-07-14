/*======================
Datastoreの
Namespace: WhatYa-Attachment-[client]-[env]
kind: SpecialTag
に対して、結果を取得しレスポンスをする
========================*/
package response

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	CONFIG "bwing.app/src/config"
	ENTITY "bwing.app/src/datastore/entity"
	QUERY "bwing.app/src/datastore/query" //共通Query
	IQUERY "bwing.app/src/datastore/query/Items"
	ERR "bwing.app/src/error"
	REQ "bwing.app/src/http/request"
	"cloud.google.com/go/datastore"
)

//Inerface
type ResSpecialTag struct{}

//AttachmentItemRefに指定がある場合に、Items検索に利用する
type AifsIndexStruct struct {
	AttachmentItemRef   string
	ResponseIndex       int
	SpcialTagItemsIndex int
}

///////////////////////////////////////////////////
/* ===========================================
//TagsWordを利用してSpecialTagのデータセットをレスポンスしTagIDを取得
	TagIDをAnsestorKeyとして、SpecialTagItemsを取得
=========================================== */
func (res ResSpecialTag) GetSpecailTagItemsByTagID(w http.ResponseWriter, rq *REQ.RequestData) {

	var err error
	var q QUERY.Queries
	var wg sync.WaitGroup

	cdt := time.Now()

	//Datastore clientを生成(このClinetを使い回す)
	client, err := datastore.NewClient(context.Background(), CONFIG.GetConfig(CONFIG.PROJECT_ID))
	if err != nil {
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
		return
	}

	//TagをPostパラメーターから摘出
	var tagsWords []string
	for _, p := range rq.PostParameter {
		if p.Name == "Query_Tags" {
			for _, t := range p.StringArray {
				tagsWords = append(tagsWords, t.Value)
			}
			break
		}
	}

	//TagIDを取得する
	var sts []ENTITY.EntitySpecialTag
	var tagIDs []*datastore.Key
	for _, t := range tagsWords {

		//Filterを設定する箱を準備（連想配列名にフィルター名、要素に値）
		args := make(map[string]string)

		//Filterに必要な、Prop名&オペ、値(文字列)を設定(latestRevisionがtrue、且つ、itemIdと一致)
		args["filter_Dflg_beq"] = "false"
		args["filter_TagsWord_eq"] = t

		//Get parameterとしてFilterを設定(Get parameterを初期化)
		REQ.SettingFilterToGetParamter(rq, &args, true)

		//SpecialTagのEntityを取得
		ens, keys, err := q.GetAllByFilter(rq, client, rq.ParamsBasic.Kind, nil)
		if err != nil {
			ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
			return
		}

		//EntityとTagID(キー(Name))を格納する
		if len(keys) != 0 {
			ens1, _ := ens.(*[]ENTITY.EntitySpecialTag)
			for i, e := range *ens1 {
				sts = append(sts, e)
				tagIDs = append(tagIDs, keys[i])
			}
		}
	}

	//Requestを格納
	var sr ENTITY.SpecialTagRequest = ENTITY.SpecialTagRequest{
		TagsWord: tagsWords}

	//SpecialTagItemsのKey, Porpの箱を準備
	var stirs []ENTITY.EntitySpecialTagItemsResponse

	//SpecialTagItemsを取得
	wg.Add(len(sts))
	for i, st := range sts {
		go getSpecialTagItems(w, rq, tagIDs[i], st.TagsWord, &stirs, &wg, client)
	}
	wg.Wait()

	//AttachmentItemRefの値と、要素位置を取得
	var aifs []AifsIndexStruct
	for i1, stir := range stirs {
		for i2, sti := range stir.SpecialTagItems {
			if sti.AttachmentItemRef != "" {
				var aif AifsIndexStruct = AifsIndexStruct{
					AttachmentItemRef:   sti.AttachmentItemRef,
					ResponseIndex:       i1,
					SpcialTagItemsIndex: i2}
				aifs = append(aifs, aif)
			}
		}
	}

	//AttachmentItemRefに指定がある(選択登録方式)結果に対して、Itemsから必要な情報を注入する
	wg.Add(len(aifs))
	for i, _ := range aifs {
		go getItems(w, rq, &aifs, i, &stirs, &wg, client)
	}
	wg.Wait()

	//処理時間計測
	fmt.Printf("Finish!! 経過(miliseconds): %vns\n", time.Since(cdt).Nanoseconds()/1000000)

	//Error Response
	if err != nil {
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
		return
	}

	//Response parse
	var sres ENTITY.EntitySpecialTagResponse = ENTITY.EntitySpecialTagResponse{
		Requests:             sr,
		SpecialTag:           sts,
		ChildSpecialTagItems: stirs,
	}

	//API response
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode((sres))
}

///////////////////////////////////////////////////
/* ===========================================
//SpecialTagを取得
//goroutine用にサブ関数として切り出し
Args
！！！！！未実装！！！！！！
=========================================== */
func getSpecialTag(w http.ResponseWriter, rq *REQ.RequestData, tagsWord string, sts *[]ENTITY.EntitySpecialTag, wg *sync.WaitGroup, client *datastore.Client) {

	defer wg.Done()

	var q QUERY.Queries

	//Filterを設定する箱を準備（連想配列名にフィルター名、要素に値）
	args := make(map[string]string)

	//Filterに必要な、Prop名&オペ、値(文字列)を設定(latestRevisionがtrue、且つ、itemIdと一致)
	args["filter_Dflg_beq"] = "false"
	args["filter_TagsWord_eq"] = tagsWord

	//Get parameterとしてFilterを設定(Get parameterを初期化)
	REQ.SettingFilterToGetParamter(rq, &args, true)

	//SpacialTagItemsを取得
	ens, _, err := q.GetAllByFilter(rq, client, rq.ParamsBasic.Kind, nil)
	if err != nil {
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
		return
	}
	ens1, _ := ens.(*[]ENTITY.EntitySpecialTag) //取得したEntityをCast

	//出力
	sts = ens1
}

///////////////////////////////////////////////////
/* ===========================================
//SpecialTagItemsを取得
//goroutine用にサブ関数として切り出し
Args

=========================================== */
func getSpecialTagItems(w http.ResponseWriter, rq *REQ.RequestData, pKey *datastore.Key, tagsWord string, stirs *[]ENTITY.EntitySpecialTagItemsResponse, wg *sync.WaitGroup, client *datastore.Client) {

	defer wg.Done()

	var q QUERY.Queries

	//Requestを格納
	var sr ENTITY.SpecialTagItemsRequest = ENTITY.SpecialTagItemsRequest{
		TagID:    pKey.Name,
		TagsWord: tagsWord}

	//SpecialTagがいない、親キーを利用していない場合
	if pKey == nil {
		//取得したSpecialTagItemsを出力
		stir := ENTITY.EntitySpecialTagItemsResponse{
			Requests:        sr,
			SpecialTagItems: nil}
		*stirs = append(*stirs, stir)
		return
	}

	//Filterを設定する箱を準備（連想配列名にフィルター名、要素に値）
	args := make(map[string]string)

	//Filterに必要な、Prop名&オペ、値(文字列)を設定(latestRevisionがtrue、且つ、itemIdと一致)
	args["filter_Dflg_beq"] = "false"

	//Get parameterとしてFilterを設定(Get parameterを初期化)
	REQ.SettingFilterToGetParamter(rq, &args, true)

	//SpacialTagItemsを祖先キーで検索し、取得
	ens, keys, err := q.GetAllByFilter(rq, client, ENTITY.KIND_SPECIALTAGITEMS, pKey)
	if err != nil {
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
		return
	}

	fmt.Println(keys)
	//取得したEntityの__KEY__(id)を格納
	var eKeys []int64
	for _, k := range keys {
		eKeys = append(eKeys, k.ID)
	}

	//取得したEntityをCast
	ens1, ok := ens.(*[]ENTITY.EntitySpecialTagItems)
	if !ok {
		err = errors.New("entities.EntitiesのCastでエラー")
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
		return
	}

	//取得したSpecialTagItemsを出力
	stir := ENTITY.EntitySpecialTagItemsResponse{
		Requests:            sr,
		SpecialTagItemsKeys: eKeys,
		SpecialTagItems:     *ens1}
	*stirs = append(*stirs, stir)
}

///////////////////////////////////////////////////
/* ===========================================
//AttachmentItemRefをもつ、SpecialTagItemsに対して、
	不足情報をItemsを取得し注入する
//goroutine用にサブ関数として切り出し
Args
=========================================== */
func getItems(w http.ResponseWriter, rq *REQ.RequestData, as *[]AifsIndexStruct, asIndex int, stirs *[]ENTITY.EntitySpecialTagItemsResponse, wg *sync.WaitGroup, client *datastore.Client) {

	defer wg.Done()

	var qi IQUERY.QueryItems

	//AttachmentItemRefからItemIdを取り出す
	itemId := (*as)[asIndex].AttachmentItemRef

	//ItemsのEntityを1件取得する
	ens, _, err := qi.GetByItemId(rq, itemId, 1, client)
	if err != nil {
		fmt.Println(err)
		return
	}
	ens1, ok := ens.(*[]ENTITY.EntityItems) //取得したEntityをCast
	if !ok {
		err = errors.New("entities.EntitiesのCastでエラー")
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
		return
	}

	//Itemsからの情報を注入する場所を指定
	rIndex := (*as)[asIndex].ResponseIndex
	sIndex := (*as)[asIndex].SpcialTagItemsIndex

	//Itemsからの情報を注入
	(*stirs)[rIndex].SpecialTagItems[sIndex].ImageMainUrls = (*ens1)[0].ImageMainUrls
	(*stirs)[rIndex].SpecialTagItems[sIndex].ImageSubUrls = (*ens1)[0].ImageSubUrls
	(*stirs)[rIndex].SpecialTagItems[sIndex].ItemDescriptionDetail = (*ens1)[0].ItemDescriptionDetail
	(*stirs)[rIndex].SpecialTagItems[sIndex].ItemDescriptionDetail2 = (*ens1)[0].ItemDescriptionDetail2
	(*stirs)[rIndex].SpecialTagItems[sIndex].ItemSiteUrl = (*ens1)[0].ItemSiteUrl
	(*stirs)[rIndex].SpecialTagItems[sIndex].ItemTitle = (*ens1)[0].ItemTitle
	(*stirs)[rIndex].SpecialTagItems[sIndex].SkuPrice = (*ens1)[0].SkuPrice
}
