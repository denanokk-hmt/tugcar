/*======================
Datastoreの
Namespace: WhatYa-Attachment-[client]-[env]
kind: SpecialTagItems
に対して、結果を取得しレスポンスをする
========================*/
package response

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	CONFIG "bwing.app/src/config"
	ENTITY "bwing.app/src/datastore/entity" //共通Query
	STQUERY "bwing.app/src/datastore/query/SpecialTag"
	STIQUERY "bwing.app/src/datastore/query/SpecialTagItems"
	ERR "bwing.app/src/error"
	REQ "bwing.app/src/http/request"
	"cloud.google.com/go/datastore"
)

//Inerface
type ResSpecialTagItems struct {
	qst  STQUERY.QuerySpecialTag
	qsti STIQUERY.QuerySpecialTagItems
}

///////////////////////////////////////////////////
/* ===========================================
__KEY__(id)とTagIDを利用してSpecialTagItemsのデータセットをレスポンスする
=========================================== */
func (res ResSpecialTagItems) GetSpecialTagItems(w http.ResponseWriter, rq *REQ.RequestData) {

	var err error
	var grs []ENTITY.SpecialTagItemsGetRequest //Requestを格納する箱
	var stis []ENTITY.EntitySpecialTagItems    //取得したSpecialTagItemsの箱
	var iws [][]string                         //SpecialTagのTagsWordを入れる箱

	cdt := time.Now()

	//IDKeyとTagIDをPostパラメーターから摘出
	for _, p := range rq.PostParameter {
		if p.Name == "Query_ItemIds" {
			for _, ids := range p.StringArray {
				//文字列分解して、int64の__KEY__(id)とTagIDを格納
				idKeys := strings.Split(ids.Value, "_")
				sIdKey := idKeys[0]
				tagID := idKeys[1]
				idKey, _ := strconv.ParseInt(sIdKey, 10, 64)
				gr := ENTITY.SpecialTagItemsGetRequest{
					IDKey: idKey,
					TagID: tagID,
				}
				grs = append(grs, gr) //Requestを格納
			}
			break
		}
	}

	//Datastore clientを生成
	client, err := datastore.NewClient(context.Background(), CONFIG.GetConfig(CONFIG.PROJECT_ID))
	if err != nil {
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
		return
	}

	//IDKeyに紐づくSpecailTagItemsのEntityを取得する
	for i, gr := range grs {

		//SpecialTagを抽出したTagIDで検索
		et, keys, err := res.qst.GetSpecialTagByTagID(rq, client, gr.TagID)
		if err != nil {
			ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
			return
		}

		//PKey情報を取得
		if len(keys) > 0 {
			grs[i].PKey = keys[0]
		}

		//Case TagsWordを取得したい
		et1 := et.(*[]ENTITY.EntitySpecialTag)

		//取得したTagsWprdをすべて格納
		var iw []string
		for _, t := range *et1 {
			iw = append(iw, t.TagsWord)
		}
		iws = append(iws, iw)
	}

	//IDKeyに紐づくSpecailTagItemsのEntityを取得する
	for i, gr := range grs {
		ens, err := res.qsti.GetSpecialTagItemsByPKeyIDKey(rq, client, gr)
		if err != nil {
			ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
			return
		}
		ens1, ok := ens.(*ENTITY.EntitySpecialTagItems) //取得したEntityをCast
		if !ok {
			ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
			return
		}
		stis = append(stis, *ens1) //SpecialTagItemsを格納
		stis[i].ItemWords = iws[i] //SpecialTagItemsにItemWordsを設置
	}

	//処理時間計測
	fmt.Printf("Finish!! 経過(miliseconds): %vms\n", time.Since(cdt).Nanoseconds()/1000000)

	//Error Response
	if err != nil {
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
		return
	}

	//Response parse
	var sres ENTITY.EntitySpecialTagItemsGetResponse = ENTITY.EntitySpecialTagItemsGetResponse{
		Requests:        grs,
		SpecialTagItems: stis}

	//API response
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode((sres))
}
