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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	BQ "bwing.app/src/bigquery/bq2ds" //共通Query
	COMMON "bwing.app/src/common"
	CONFIG "bwing.app/src/config"
	ENTITY "bwing.app/src/datastore/entity" //共通Query
	"cloud.google.com/go/datastore"

	IGQUERY "bwing.app/src/datastore/query/IgnoreItems" //IgnoreItems専用Query
	IXQUERY "bwing.app/src/datastore/query/ItemIndex"   //ItemIndex専用Query
	IQUERY "bwing.app/src/datastore/query/Items"        //Items専用Query
	TQUERY "bwing.app/src/datastore/query/Tags"         //Tags専用Query
	ERR "bwing.app/src/error"
	REQ "bwing.app/src/http/request"
)

// Inerface
type ResItems struct{}

var qi IQUERY.QueryItems
var qt TQUERY.QueryTags
var qix IXQUERY.QueryItemIndex
var qig IGQUERY.QueryIgnoreItems

///////////////////////////////////////////////////
/* =========================================== */
//Items データをBigQueryからロード
//SQLの拡張性を考慮して、マスタ別にこのResponseを準備する
/* =========================================== */
func (res ResItems) LoadItemsData(w http.ResponseWriter, r *http.Request, rq *REQ.RequestData) (int, error) {

	cdt := time.Now()

	//BigQueryのDataMartから取得したデータを、Datastoreにロードする
	resultsQty, err := BQ.FetchBigQueryLoad2Ds(rq)
	if err != nil {
		return 0, err
	}

	//処理時間計測
	fmt.Printf("Finish!! 経過(seconds): %vns\n", time.Since(cdt).Seconds())

	return resultsQty, nil
}

///////////////////////////////////////////////////
/* =========================================== */
//Items ロードデータを更新
//更新処理の拡張性を考慮して、マスタ別にこのResponseを準備する
/* =========================================== */
func (res ResItems) UpdateItemsData(w http.ResponseWriter, r *http.Request, rq *REQ.RequestData, filters []REQ.Filter) (int, error) {

	cdt := time.Now()

	var err error

	//Datastore clientを生成(このClinetを使い回す)
	client, err := datastore.NewClient(context.Background(), CONFIG.GetConfig(CONFIG.PROJECT_ID))
	if err != nil {
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
		return 0, err
	}

	//Entityを更新:既存の有効Revisionを無効に更新(LatestRevision=FALSE)
	resultsQty, err := qi.UpdateLatestRevisionByRevision(rq, client, filters[:2], false)
	if err != nil {
		return 0, err
	}

	//Entityを更新:指定したRevisionを有効に更新(LatestRevision=TRUE)
	resultsQty, err = qi.UpdateLatestRevisionByRevision(rq, client, filters[2:], true)
	if err != nil {
		return 0, err
	}

	//処理時間計測
	fmt.Printf("Finish!! 経過(seconds): %vns\n", time.Since(cdt).Seconds())

	return resultsQty, nil
}

///////////////////////////////////////////////////
/* ===========================================
//TagsWordからItemIndexを利用してItemsのデータセットをレスポンスする

一投目をItemIdでも、TagsWordで投げても利用できる。これはTagsにItemIdもTagsWordの一つとして
必ず存在させているため
[RossetaStone:]

=========================================== */
func (res ResItems) GetItemsByTagsWord(w http.ResponseWriter, rq *REQ.RequestData) {

	/*------------------------------------------------
	事前準備
	------------------------------------------------*/

	var err error
	var wg sync.WaitGroup  //goroutin 準備
	var sf ResItemsSubFunc //サブ関数呼び出し

	cdt := rq.ParamsBasic.Cdt //処理開始時間
	cdtTmp := time.Now()
	unixmilli := cdt.UnixNano() / 1000000 //unixtime millisecond(13digit)

	//Datastore clientを生成
	client, err := datastore.NewClient(context.Background(), CONFIG.GetConfig(CONFIG.PROJECT_ID))
	if err != nil {
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
		return
	}

	//TagをPostパラメーターから摘出
	var tag []string
	for _, p := range rq.PostParameter {
		if p.Name != "Query_Tags" {
			continue
		}
		//Query_Tagsパラメーターに格納されたをタグをすべて取得
		for _, t := range p.StringArray {
			tag = append(tag, t.Value)
		}
		break
	}

	/*------------------------------------------------
	箱の準備
	------------------------------------------------*/
	var items1st ENTITY.EntityItems   //箱から取り出してレスポンス向けの箱
	var categoryL int                 //itemss1stのCategory_L
	var categoryS int                 //itemss1stのCategory_S
	var itemIdss []ENTITY.ItemIds     //ItemIndexを複数入れる箱
	var tr ENTITY.TagsRequest         //リクエストされたタグを返却する箱
	var tw ENTITY.TagsWord            //タグ情報を入れる箱
	var ig []ENTITY.EntityIgnoreItems //Ignoreを入れる箱
	var items []ENTITY.EntityItems    //Itemsを入れる箱
	var eixs []ENTITY.EntityItemIndex

	/*------------------------------------------------
	TagsWordでTagsを検索-->ItemIdを取得-->ItemIdからItemIndexを検索-->ItemIdsを取得
	------------------------------------------------*/

	//パラメーターのタグでループ
	for _, t := range tag {

		//レスポンス用の検索TagsWordを格納
		tw.Value = t
		tr.TagsWords = append(tr.TagsWords, tw)

		//TagsWordに紐づくItemIdを取得
		e, err := qt.GetItemIdByTagsWord(rq, t, client)
		if err != nil {
			ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
			return
		}
		etags, _ := e.(*[]ENTITY.EntityTags) //取得したEntityをCast

		//初期取得項目(SearchしたItems情報(ItemIdタグでの検索)と、IgnoreItems情報)
		if len(*etags) > 0 {
			wg.Add(2)
			//ItemIdを検索値ItemIdとして確保
			go sf._getSearachItemByItemId(w, rq, &items1st, (*etags)[0].ItemId, &categoryL, &categoryS, client, &wg)
			go sf._getIgnoreItems(w, rq, &ig, unixmilli, client, &wg)
			wg.Wait()
		}

		//取得したTagsを決定(速度海鮮(https://svc.atlassian.net/browse/APP-1053)の暫定対策)
		var searchTags []ENTITY.EntityTags
		if len(*etags) < ENTITY.SEARCH_TAGS_BLOCK_QTY {
			searchTags = *etags
		} else {
			searchTags = (*etags)[:ENTITY.SEARCH_TAGS_BLOCK_QTY] //-->今後の検討POINT!!
		}

		//取得したTagsをループ(ヒットしたTagsからItemIdを検索)
		wg.Add(len(searchTags))
		for _, et := range searchTags {

			//ItemIdでItemIndexを検索、ItemIdsを取得
			go func(rq *REQ.RequestData, eixs *[]ENTITY.EntityItemIndex, itemId string, client *datastore.Client, wg *sync.WaitGroup) {
				defer wg.Done()
				e2, _, err := qix.GetByItemId(rq, itemId, client)
				if err != nil {
					fmt.Println(err)
				}
				eix, _ := e2.(*[]ENTITY.EntityItemIndex) //取得したEntityをCast
				if len(*eix) > 0 {
					*eixs = append(*eixs, (*eix)[0])
				}
			}(rq, &eixs, et.ItemId, client, &wg)
		}
		wg.Wait()
	}

	//IgnoreItemsを除外、重複ItemIdを除外
	//ItemIdとFrequencyとDepthだけにする-->Items検索用として確保
	for _, it := range eixs {
		for _, ids := range it.ItemIds {
			if sf._checkerIgnoreItemId(ig, ids.Value) {
				if sf._checkerUniqueItemId(itemIdss, ids.Value) {
					var itemIds ENTITY.ItemIds
					itemIds.Value = ids.Value
					itemIds.Frequency = ids.Frequency
					itemIds.Depth = ids.Depth
					itemIdss = append(itemIdss, itemIds)
				}
			}
		}
	}
	fmt.Printf("ItemIndex取得!! 経過(miliseconds): %vms\n", time.Since(cdtTmp).Nanoseconds()/1000000)

	/*------------------------------------------------
	ItemIdsでItemsを検索-->Itemsを取得
	------------------------------------------------*/

	//ItemIdをAscでソート
	sort.SliceStable(itemIdss, func(i, j int) bool { return itemIdss[i].Value < itemIdss[j].Value }) //asc

	//Item検索関数をWorkerに登録
	var workers int = 2
	lenItemIdss := len(itemIdss)
	switch {
	case lenItemIdss > 10 && lenItemIdss < 50:
		workers = lenItemIdss / 2
	default:
		workers = ENTITY.SEARCH_ITEMS_MAX_WORKERS
	}
	wfMap := sf._setGetItemsByItemIdWorkerFuncMapping(workers)

	//各Workerに配る要素位置を設定
	s, e := COMMON.SliceDevideCalculator(len(itemIdss), len(wfMap))

	//Itemsを取得する
	wg.Add(len(wfMap))
	for n, wf := range wfMap {
		go func(wg *sync.WaitGroup, n int, wf func(REQ.RequestData, *[]ENTITY.EntityItems, string, int, int, int, int, *datastore.Client, chan bool)) {
			defer wg.Done()
			for _, itemIds := range itemIdss[s[n]:e[n]] {
				ch := make(chan bool)
				go wf(*rq, &items, itemIds.Value, itemIds.Frequency, itemIds.Depth, categoryL, categoryS, client, ch)
				select {
				case <-ch:
					//fmt.Ptintln(<-ch)
				}
			}
		}(&wg, n, wf)
	}
	wg.Wait()

	//Error Response
	if err != nil {
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
		return
	}

	/*------------------------------------------------
	レスポンスを整形
	------------------------------------------------*/

	//Mapにたたみ直し、goroutineでの検索をすべて閉じ込める
	itemsMaps := make(map[string][]ENTITY.EntityItems)
	for _, i := range items {
		itemsMaps[i.ItemId] = append(itemsMaps[i.ItemId], i)
	}

	//重複ItemIdsを削除しユニークにする
	var itemsUniq []ENTITY.EntityItems
	for _, m := range itemsMaps {
		itemsUniq = append(itemsUniq, m[0])
	}

	//Orsersレスポンス(昇順降順設定)
	var orders []ENTITY.Order
	order := ENTITY.Order{Name: "ItemId", Value: "asc"}
	orders = append(orders, order)
	sort.SliceStable(itemsUniq, func(i, j int) bool { return itemsUniq[i].ItemId < itemsUniq[j].ItemId }) //ItemIdをasc

	//Response parse
	var ires ENTITY.EntityItemssResponse = ENTITY.EntityItemssResponse{
		Requests:    tr,
		SearchItems: items1st,
		Orders:      orders,
		Items:       itemsUniq}

	//処理時間計測
	fmt.Printf("Finish!! 経過(miliseconds): %vms\n", time.Since(cdt).Nanoseconds()/1000000)

	//API response
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode((ires))
}

///////////////////////////////////////////////////
/* ===========================================
//ItemIdからItemsのデータセットをレスポンスする
[RossetaStone:]

=========================================== */
func (res ResItems) GetItemsByItemId(w http.ResponseWriter, rq *REQ.RequestData) {

	var err error

	//Datastore clientを生成(このClinetを使い回す)
	client, err := datastore.NewClient(context.Background(), CONFIG.GetConfig(CONFIG.PROJECT_ID))
	if err != nil {
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
		return
	}

	//goroutin 準備
	var wg sync.WaitGroup

	//処理時間
	cdt := rq.ParamsBasic.Cdt

	//unixtime millisecond(13digit)
	unixmilli := cdt.UnixNano() / 1000000

	//ItemIdをPostパラメーターから摘出
	var itemIds []string
	for _, p := range rq.PostParameter {
		if p.Name != "Query_ItemIds" {
			continue
		}
		//Query_Tagsパラメーターに格納されたをタグをすべて取得
		for _, id := range p.StringArray {
			itemIds = append(itemIds, id.Value)
		}
		break
	}

	//箱を準備
	var sf ResItemsSubFunc
	var ir ENTITY.ItemIdsRequest       //リクエストされたタグを返却する箱
	var igs []ENTITY.EntityIgnoreItems //Ignoreを入れる箱
	var searchItemIds []string         //重複やignoreを抜いて検索するItemIdの箱
	var getItemIds []string            //検索した結果取得出来たItemIdの箱
	var items []ENTITY.EntityItems     //Itemsを入れる箱

	//レスポンス用の検索ItemIdを格納
	ir.ItemIds = itemIds

	//重複削除
	itemIds = sf._removeDuplicate(itemIds)

	//Ignore Itemsを取得
	wg.Add(1)
	go sf._getIgnoreItems(w, rq, &igs, unixmilli, client, &wg)
	wg.Wait()

	//ItemIdsからIgnoreを除外
	for _, id := range itemIds {
		ignoreFlg := false
		for _, ig := range igs {
			if id == ig.Id {
				ignoreFlg = true
				break
			}
		}
		if !ignoreFlg {
			searchItemIds = append(searchItemIds, id)
		}
	}

	//Item検索関数をWorkerに登録
	var workers int = 2
	lenSearchItemIds := len(searchItemIds)
	switch {
	case lenSearchItemIds > 10 && lenSearchItemIds < 50:
		workers = lenSearchItemIds / 2
	default:
		workers = ENTITY.SEARCH_ITEMS_MAX_WORKERS
	}
	wfMap := sf._setGetItemsByItemIdWorkerFuncMapping(workers)

	//各Workerに配る要素位置を設定
	s, e := COMMON.SliceDevideCalculator(len(searchItemIds), len(wfMap))

	//Itemsを取得する
	wg.Add(len(wfMap))
	for n, wf := range wfMap {
		go func(wg *sync.WaitGroup, n int, wf func(REQ.RequestData, *[]ENTITY.EntityItems, string, int, int, int, int, *datastore.Client, chan bool)) {
			defer wg.Done()
			for _, id := range itemIds[s[n]:e[n]] {
				ch := make(chan bool)
				categoryL := 0
				categoryS := 0
				go wf(*rq, &items, id, 0, 0, categoryL, categoryS, client, ch)
				select {
				case <-ch:
					//fmt.Ptintln(<-ch)
				}
			}
		}(&wg, n, wf)
	}
	wg.Wait()

	//Error Response
	if err != nil {
		ERR.ErrorResponse(w, rq, err, http.StatusInternalServerError)
		return
	}

	//Mapにたたみ直し、goroutineでの検索をすべて閉じ込める
	itemsMaps := make(map[string][]ENTITY.EntityItems)
	for _, i := range items {
		itemsMaps[i.ItemId] = append(itemsMaps[i.ItemId], i)
		getItemIds = append(getItemIds, i.ItemId)
	}

	//重複ItemIdsを削除しユニークにする
	var itemsUniq []ENTITY.EntityItems
	for _, m := range itemsMaps {
		itemsUniq = append(itemsUniq, m[0])
	}

	//Orsersレスポンス(昇順降順設定)
	var orders []ENTITY.Order
	order := ENTITY.Order{Name: "ItemId", Value: "asc"}
	orders = append(orders, order)
	sort.SliceStable(itemsUniq, func(i, j int) bool { return itemsUniq[i].ItemId < itemsUniq[j].ItemId }) //ItemIdをasc

	//Response parse
	var ires ENTITY.EntityItemIdsResponse = ENTITY.EntityItemIdsResponse{
		Requests:   ir,
		GetItemIds: getItemIds,
		Orders:     orders,
		Items:      itemsUniq}

	//処理時間計測
	fmt.Printf("Finish!! 経過(miliseconds): %vms\n", time.Since(cdt).Nanoseconds()/1000000)

	//API response
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode((ires))
}
