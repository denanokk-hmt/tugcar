/*
======================
Datastoreの
Namespace: WhatYa-Attachment-[client]-[env]
kind: Items
に対して、結果を取得しレスポンスをする
========================
*/
package response

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
	"sync"

	ENTITY "bwing.app/src/datastore/entity"
	"cloud.google.com/go/datastore"

	ERR "bwing.app/src/error"
	REQ "bwing.app/src/http/request"
)

// Interface
type ResItemsSubFunc struct{}

///////////////////////////////////////////////////
/* ===========================================
//検索に利用したItemsのみを取得する
//goroutine用にサブ関数として切り出し
Args

=========================================== */
func (sf *ResItemsSubFunc) _getSearachItemByItemId(w http.ResponseWriter, rq *REQ.RequestData,
	items1st *ENTITY.EntityItems, itemId string, categoryL *int, categoryS *int,
	client *datastore.Client, wg *sync.WaitGroup) {

	defer wg.Done()

	//Search Itemsを取得
	e, _, err := qi.GetByItemId(rq, itemId, -1, client)
	if err != nil {
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
		return
	}

	//取得したEntityをCast
	ei, _ := e.(*[]ENTITY.EntityItems)

	//entityをレスポンス向けに確保したいので、ループして取り出しつつ、CategoryLとSを確保
	for _, it := range *ei {
		*categoryL = it.ItemCategoryCodeL
		*categoryS = it.ItemCategoryCodeS
		*items1st = it
		break
	}
}

///////////////////////////////////////////////////
/* ===========================================
//IgnoreのItemIdを取得
//goroutine用にサブ関数として切り出し
Args

=========================================== */
func (sf *ResItemsSubFunc) _getIgnoreItems(w http.ResponseWriter, rq *REQ.RequestData,
	ig *[]ENTITY.EntityIgnoreItems, unixmilli int64,
	client *datastore.Client, wg *sync.WaitGroup) {

	defer wg.Done()

	//ItemsのIgnoreを取得
	e, _, err := qig.GetIgnoreItems(rq, client)
	if err != nil {
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
		return
	}
	eig, _ := e.(*[]ENTITY.EntityIgnoreItems)

	//ignoreItemsを除外(DSでの不等号を一度に2つ重ねられない≒Dflgのみで取得し、ここではじく)
	for _, i := range *eig {
		if int64(i.IgnoreSinceUnixtime) <= unixmilli && unixmilli <= int64(i.IgnoreUntilUnixtime) {
			*ig = append(*ig, i)
		}
	}
}

///////////////////////////////////////////////////
/* ===========================================
//関数_getItemsByItemIdWithChannelをWorkerをセット
=========================================== */
func (sf *ResItemsSubFunc) _setGetItemsByItemIdWorkerFuncMapping(workerQty int) map[int]func(REQ.RequestData, *[]ENTITY.EntityItems, string, int, int, int, int, *datastore.Client, chan bool) {
	wfMap := make(map[int]func(REQ.RequestData, *[]ENTITY.EntityItems, string, int, int, int, int, *datastore.Client, chan bool))
	for i := 0; i < workerQty; i++ {
		var wf func(REQ.RequestData, *[]ENTITY.EntityItems, string, int, int, int, int, *datastore.Client, chan bool)
		wf = getItemsByItemIdWithChannel
		wfMap[i] = wf
		//fmt.Println(&wf) address check
	}
	return wfMap
}

///////////////////////////////////////////////////
/* ===========================================
//Sync WatiGroupを使ってItemsを取得する
Args

=========================================== */
func (sf *ResItemsSubFunc) _getItemsByItemIdWithSync(rq REQ.RequestData,
	items *[]ENTITY.EntityItems, itemId string, frequency int, depth int, categoryL int, categoryS int,
	client *datastore.Client, wg *sync.WaitGroup) {
	//ch chan bool) {

	//fmt.Println("=============START===========" + itemId)
	//defer close(ch)
	defer wg.Done()

	var err error

	//ItemsのEntityを1件取得する
	e1, pKey, err := qi.GetByItemId(&rq, itemId, 1, client)
	if err != nil {
		fmt.Println(err)
		return
	}
	ei, _ := e1.(*[]ENTITY.EntityItems) //取得したEntityをCast

	//ItemsのEntityが取得出来なかった場合、去る
	if pKey == nil {
		fmt.Println("no entity", itemId, "func():postResponseItems.go/getItemsByItemId()")
		return
	}

	//Itemsのキーを親キーとして、TagsWordを取得
	e2, err := qt.GetTagsWordByItemId(&rq, "", pKey[0], client)
	if err != nil {
		fmt.Println(err)
		return
	}
	et, _ := e2.(*[]ENTITY.EntityTags) //取得したEntityをCast

	//取得したTagsWprdをすべて格納
	var words []string
	for _, t := range *et {
		if t.LatestRevision {
			words = append(words, t.TagsWord)
		}
	}

	//Filtersを適用、合致したものに対して、
	//Depth値を追加して、Itemsを格納
	//Category差分を算出してItemsに格納
	//WordsをItemsに格納
	for _, e := range *ei {
		if _checkerFilters(&rq, &e) {
			e.ItemCategoryCodeLSearchCalc = int(math.Abs(float64(categoryL - e.ItemCategoryCodeL)))
			e.ItemCategoryCodeSSearchCalc = int(math.Abs(float64(categoryS - e.ItemCategoryCodeS)))
			e.Frequency = frequency
			e.Depth = depth
			e.ItemWords = words
			*items = append(*items, e)
		}
	}
	//fmt.Println("=============END===========" + itemId)
	//ch <- true
}

///////////////////////////////////////////////////
/* ===========================================
//Sync WatiGroupを使ってItemsを取得する
Args

=========================================== */
func getItemsByItemIdWithChannel(rq REQ.RequestData,
	items *[]ENTITY.EntityItems, itemId string, frequency int, depth int, categoryL int, categoryS int,
	client *datastore.Client, ch chan bool) {

	//fmt.Println("=============START===========" + itemId)
	defer close(ch)

	var err error

	//ItemsのEntityを1件取得する
	e1, pKey, err := qi.GetByItemId(&rq, itemId, 1, client)
	if err != nil {
		fmt.Println(err)
		return
	}
	ei, _ := e1.(*[]ENTITY.EntityItems) //取得したEntityをCast

	//ItemsのEntityが取得出来なかった場合、去る
	if pKey == nil {
		fmt.Println("no entity", itemId, "func():postResponseItems.go/getItemsByItemId()")
		return
	}

	//Tagsを親キーで検索-->TagsWprdを取得
	e2, err := qt.GetTagsWordByItemId(&rq, "", pKey[0], client)
	if err != nil {
		fmt.Println(err)
		return
	}
	et, _ := e2.(*[]ENTITY.EntityTags) //取得したEntityをCast

	//取得したTagsWprdをすべて格納
	var words []string
	for _, t := range *et {
		if t.LatestRevision {
			words = append(words, t.TagsWord)
		}
	}

	//Filtersを適用、合致したものに対して、
	//Depth値を追加して、Itemsを格納
	//Category差分を算出してItemsに格納
	//WordsをItemsに格納
	for _, e := range *ei {
		if _checkerFilters(&rq, &e) {
			e.ItemCategoryCodeLSearchCalc = int(math.Abs(float64(categoryL - e.ItemCategoryCodeL)))
			e.ItemCategoryCodeSSearchCalc = int(math.Abs(float64(categoryS - e.ItemCategoryCodeS)))
			e.Frequency = frequency
			e.Depth = depth
			e.ItemWords = words
			*items = append(*items, e)
		}
	}
	//fmt.Println("=============END===========" + itemId)
	ch <- true
}

///////////////////////////////////////////////////
/* ===========================================
//重複を削除する
=========================================== */
func (sf *ResItemsSubFunc) _removeDuplicate(args []string) []string {
	results := make([]string, 0, len(args))
	encountered := map[string]bool{}
	for i := 0; i < len(args); i++ {
		if !encountered[args[i]] {
			encountered[args[i]] = true
			results = append(results, args[i])
		}
	}
	return results
}

///////////////////////////////////////////////////
/* ===========================================
//ItemId::確保している要素に対してIgnore指定かどうかを確認
=========================================== */
func (sf *ResItemsSubFunc) _checkerIgnoreItemId(ignores []ENTITY.EntityIgnoreItems, targetItemId string) bool {
	judge := true
	for _, ig := range ignores {
		if ig.Id == targetItemId {
			judge = false
			break
		}
	}
	return judge
}

///////////////////////////////////////////////////
/* ===========================================
//ItemId::確保している要素に対してUniqかどうかを確認
=========================================== */
func (sf *ResItemsSubFunc) _checkerUniqueItemId(itemIdss []ENTITY.ItemIds, targetItemId string) bool {
	judge := true
	for _, ids := range itemIdss {
		if ids.Value == targetItemId {
			judge = false
			break
		}
	}
	return judge
}

///////////////////////////////////////////////////
/* ===========================================
//Filters::フィルター判定(指定された項目がすべてあればTRUE)
=========================================== */
func _checkerFilters(rq *REQ.RequestData, ens *ENTITY.EntityItems) bool {

	var j bool = true
	var s string

	//Filters(パラメーター)をDSのKind名でCastして取り出す
	filters := rq.Filters[rq.ParamsBasic.Kind].Filters

	//比較演算関数をDSのKind名でCastして取り出す
	comparison := rq.Filters[rq.ParamsBasic.Kind].Comparison

	//FiltersのNameからDSのValueを取得し文字列に変換
	for _, f := range filters {
		switch f.Name {
		case "sex":
			s = strconv.Itoa(ens.ItemSex)
		case "category_code_L", "category_code_l":
			s = strconv.Itoa(ens.ItemCategoryCodeL)
		case "category_code_S", "category_code_s":
			s = strconv.Itoa(ens.ItemCategoryCodeS)
		case "brand_code":
			s = strconv.Itoa(ens.ItemBrandCode)
		}

		//commonの比較演算関数で値の比較
		if j = comparison(s, f.Value, f.Ope); !j {
			break
		}
	}
	return j
}
