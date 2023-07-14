/*======================
GET Methodリクエストに対する処理を行わせ、結果をレスポンスする
========================*/
package response

import (
	"encoding/json"
	"net/http"
	"reflect"

	ENTITY "bwing.app/src/datastore/entity"
	QUERY "bwing.app/src/datastore/query"
	AQUERY "bwing.app/src/datastore/query/Article" //Article専用Query

	ERR "bwing.app/src/error"
	REQ "bwing.app/src/http/request"

	IRES "bwing.app/src/http/response/datastore/Items"             //Items専用Query
	SRES "bwing.app/src/http/response/datastore/Sku"               //Sku専用
	STRES "bwing.app/src/http/response/datastore/SpecialTag"       //SpecialTag専用
	STIRES "bwing.app/src/http/response/datastore/SpecialTagItems" //SpecialTagItems専用
)

//Inerface
type GetResponse struct{}

///////////////////////////////////////////////////
//Entitiesの中身を抽出する
func entitiesReflect(entities ENTITY.Entities) []*interface{} {

	//構造体のリフレクションを取得
	rtCst := reflect.TypeOf(entities)

	//構造体の値を取得
	rvCst := reflect.ValueOf(entities)

	//構造体の全フィールドを取得し、、、
	var vv []*interface{}
	for i := 0; i < rtCst.NumField(); i++ {
		//ENTITY情報を取得(1件のみのはず=0番目)
		f := rtCst.Field(i)
		// FieldByNameメソッドでフィールド名に対応する値を取得
		v := rvCst.FieldByName(f.Name).Interface()
		vv = append(vv, &v)
	}

	return vv
}

///////////////////////////////////////////////////
/* =========================================== */
//APIに応じてDatastoreからEntityを取得
/* =========================================== */
func (res GetResponse) GetDsEntities(w http.ResponseWriter, rq *REQ.RequestData) {

	//レスポンスヘッダーを先に設定
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	var err error

	switch rq.ParamsBasic.Kind {

	//Itemsを取得
	case ENTITY.KIND_ITEMS:
		var qi IRES.ResItems
		if rq.ParamsBasic.Action == REQ.ACTION_PATH_GET {
			//Getの場合、ダイレクトにItemIdや__Key__など固有を取得しにいく場合
			qi.GetItemsByItemId(w, rq)
		} else {
			//Searchの場合、TagsWordからのItemId検索
			qi.GetItemsByTagsWord(w, rq)
		}

	//Skuを取得
	case ENTITY.KIND_SKU:
		var qs SRES.ResSku
		qs.GetSkuByItemId(w, rq)

	//SpecialTagからSpecialTagItemsを取得
	case ENTITY.KIND_SPECIALTAG:
		var qst STRES.ResSpecialTag
		qst.GetSpecailTagItemsByTagID(w, rq)

	//SpecialTagItemsを取得
	case ENTITY.KIND_SPECIALTAGITEMS:
		var qsti STIRES.ResSpecialTagItems
		if rq.ParamsBasic.Action == REQ.ACTION_PATH_GET {
			//Getの場合、ダイレクトにItemIdや__Key__など固有を取得しにいく場合
			qsti.GetSpecialTagItems(w, rq)
		}

	default:
		//通常の場合のGetメソッドからのentity取得処理
		//var entities ENTITY.Entities
		var q QUERY.Queries
		entities, _, _, err := q.GetAllByFilterIncludeDsCient(rq)
		if err == nil {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode((entities))
		}
	}
	//Error Response
	if err != nil {
		ERR.ErrorResponse(w, rq, err, http.StatusBadRequest)
	}
}

/*
	=================================================================================
	これより以下、サーバー初期構築時につくられたサンプル実装
	=================================================================================
*/

///////////////////////////////////////////////////
/* =========================================== */
//共通化ができなかったパターン
//フィルターやオーダーを指定して、Runを使って、Entityを複数件取得(key情報の返却あり)
/* =========================================== */
func (res GetResponse) GetRunByFilter(w http.ResponseWriter, rq *REQ.RequestData) {
	//Get Entity
	var q AQUERY.QueryArticle
	entities, err := q.GetRunByFilterWithKey(rq)
	//Response
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if err != nil {
		ERR.ErrorResponse(w, rq, err, http.StatusBadRequest)
	} else {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode((entities))
	}
}
