/*
======================
Datastoreの
Namespace: WhatYa-Attachment-[client]-[env]
kind: ItemIndex
ItemsとTags からWordChainを形成し、ItemIndexへロード
========================
*/
package bigquery

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strconv"
	"sync"

	CONFIG "bwing.app/src/config"
	ENTITY "bwing.app/src/datastore/entity"
	QUERY "bwing.app/src/datastore/query"             //共通Query
	IXQUERY "bwing.app/src/datastore/query/ItemIndex" //ItemIndex専用Query
	REQ "bwing.app/src/http/request"
	"cloud.google.com/go/datastore"
)

// Inerface
type ResItemIndex struct{}

// Query
var q QUERY.Queries
var qix IXQUERY.QueryItemIndex

///////////////////////////////////////////////////
/* ===========================================
//BigQueryからロード済みのItemsとTagsのデータからItemIndexを形成する
	前提；ItemsとTagsがロード済みであること
残タスク
！！！Updateの500 chunk
！！！レスポンス（結果

=========================================== */
func (res ResItemIndex) Load2DsItemIdIndex(rq *REQ.RequestData) (int, error) {

	var err error

	/*------------------------------------------------
	事前準備
	//最新のTagsとItemsのLoadデータを取得
	//ループ数を確認し、goroutineを設定
	//Indexを収める各種箱の準備
	------------------------------------------------*/

	//Datastore clientを生成(このClinetを使い回す)
	client, err := datastore.NewClient(context.Background(), CONFIG.GetConfig(CONFIG.PROJECT_ID))
	if err != nil {
		return 0, err
	}
	defer client.Close()

	//Items、TagsのRevision指定がある場合、確保
	var itemsRevision, tagsRevision int
	for _, p := range rq.PostParameter {
		switch p.Name {
		case "ItemsRevision":
			itemsRevision = p.IntValue
		case "TagsRevision":
			tagsRevision = p.IntValue
		}
	}

	//すべてのItemsデータをDsから取得する
	ei, _, err := q.GetAllByRevision(rq, itemsRevision, -1, client, ENTITY.KIND_ITEMS)
	if err != nil {
		return 0, err
	}
	itemsAll, _ := ei.(*[]ENTITY.EntityItems) //取得したEntityをCast

	//TagsのRevision指定がない場合、Latestを1件取得し、TagsのFilterに用いる最新Revisionを取得
	if tagsRevision == 0 {
		et, _, err := q.GetAllByRevision(rq, 0, 1, client, ENTITY.KIND_TAGS)
		le, _ := et.(*[]ENTITY.EntityTags) //取得したEntityをCast
		if err != nil {
			return 0, err
		} else if len(*le) == 0 {
			err = errors.New("tags entitis not exits")
			return 0, err
		}
		tagsRevision = (*le)[0].Revision //Revisionを取得
	}

	//RevisionすべてのTagsデータをDsから取得する
	eta, _, err := q.GetAllByRevision(rq, tagsRevision, -1, client, ENTITY.KIND_TAGS)
	if err != nil {
		return 0, err
	}
	tagsAll, _ := eta.(*[]ENTITY.EntityTags) //取得したEntityをCast

	//TagsWordがItemIdと一致するEntityを取得
	var tagsItemIds []ENTITY.EntityTags
	for _, t := range *tagsAll {
		if t.ItemId == t.TagsWord {
			tagsItemIds = append(tagsItemIds, t)
		}
	}

	//最終結果の箱の準備
	var IIss []ENTITY.ItemIdItemIds //回帰処理でItemIdに紐付いたItemIdを確保する箱(ItemIds propに登録される)

	/*------------------------------------------------
	WordChain Index作成工程
	//ItemsとTagsを回帰的に検索処理を行って、WordChainを形成
	//TagsWordで検索→紐づくItemIdを取得=Indexに追加：これをひたすらに行う
	//DS(ItemIndex)へロードする
	------------------------------------------------*/

	//ItemIndexを作成し、DatastoreへLoadする回帰処理
	//※すべてのTagには、ワードと共にItemIdを紐付け、TagsWordとして登録している。→先にtagsItemIdsに格納
	//これを検索(回帰)の起点としてTagsWordの繋がり(Index)を生成する
	for idx, t := range tagsItemIds {
		fmt.Println("==========START::" + t.TagsWord)

		//確保するItemIdを準備
		var IIs ENTITY.ItemIdItemIds = ENTITY.ItemIdItemIds{SearchItemId: t.ItemId}

		//確保するItemIdのソートを準備
		for _, p := range rq.ParamsBasic.Orders {
			IIs.SearchOrders = append(IIs.SearchOrders, ENTITY.ItemsOrder{Name: p.Name, Value: p.Value})
		}

		//TagsのTagsWordを使って、Tagsを検索し、チェーンされるItemId達を取得し格納していく（回帰処理）
		ch := make(chan bool)
		go getItemIndex(rq, tagsRevision, idx, tagsAll, itemsAll, t.TagsWord, &IIs, &IIss, client, ch)

		fmt.Println(<-ch) //処理が飛ばないようにくさびを打つ
		fmt.Println("==========END::" + t.TagsWord)
	}

	//Mapにたたみ直し、goroutineでの検索をすべて閉じ込める
	itemIdsMaps := make(map[string][]ENTITY.ItemIdItemIds)
	for _, i := range IIss {
		itemIdsMaps[i.SearchItemId] = append(itemIdsMaps[i.SearchItemId], i)
	}

	//重複ItemIdsを削除しユニークにする
	var itemIdsUniq []ENTITY.ItemIdItemIds
	for _, m := range itemIdsMaps {
		itemIdsUniq = append(itemIdsUniq, m[0])
	}

	//Load数
	loadQty := len(itemIdsUniq)

	//goroutine, sync
	var wg sync.WaitGroup
	wg.Add(loadQty)

	//完成したItemIndexをDsへロード
	for _, iis := range itemIdsUniq {
		go load2DsItemIdIndex(rq, iis, client, &wg)
	}
	wg.Wait()

	/*------------------------------------------------
	追加更新
	//ロードされたItemIndexデータを取得し、各ItemIdsがもつ、Depthから切り捨て判定する値を算出
	//判定値にあわせてItemIdsを切り捨て調整し、DSを上書きする
	------------------------------------------------*/
	err = updateItemIdsByDepthCutter(rq, client)
	if err != nil {
		return 0, err
	}

	/*------------------------------------------------
	LatestRevision管理
	//古いロードデータのLatestRevisionをFALSEに更新
	//LatestRevisionパラメーターを取得: trueの場合、LatestRevisionを更新
	------------------------------------------------*/
	if rq.ParamsBasic.LatestRevision {
		err = updateLatestRevision(rq, client)
		if err != nil {
			return 0, err
		}
	}

	//ロード結果
	resultsQty := loadQty

	return resultsQty, err
}

///////////////////////////////////////////////////
/* =========================================== */
//ItemIdのIndex ItemIdsをLoadしていく　　（Request payload size exceeds the limit: 11534336 bytes.
/* =========================================== */
func getItemIndex(
	rq *REQ.RequestData,
	latestRev,
	idx int,
	tagsAll *[]ENTITY.EntityTags,
	itemsAll *[]ENTITY.EntityItems,
	tagsWord string,
	IIs *ENTITY.ItemIdItemIds,
	IIss *[]ENTITY.ItemIdItemIds,
	client *datastore.Client,
	ch chan bool) error {

	var err error
	defer close(ch)

	//第一階層指定
	IIs.Depth = 0

	//TagsWordを使って、Tagsを検索し、紐づくItemIdを取得してItemIdsに格納する
	err = recursiveGetTagsByTagsWord(rq, latestRev, tagsAll, itemsAll, tagsWord, IIs, IIss, 0, ch)
	if err != nil {
		log.Fatal(err, tagsWord, IIs)
		return err
	}

	//パラメーターで指定したソートオーダーに乗っ取りItemIds内の順番を変更する
	for io, p := range rq.ParamsBasic.Orders {
		for _, s := range IIs.SearchOrders {
			if p.Name == s.Name {
				if s.Value == "asc" {
					sort.SliceStable(IIs.ItemIds, func(i, j int) bool { return IIs.ItemIds[i].Orders[io].Value < IIs.ItemIds[j].Orders[io].Value })
				} else {
					sort.SliceStable(IIs.ItemIds, func(i, j int) bool { return IIs.ItemIds[i].Orders[io].Value > IIs.ItemIds[j].Orders[io].Value })
				}
			}
		}
	}

	//Load結果を出力
	cdt := rq.ParamsBasic.Cdt
	var resultOutPut ENTITY.LoadItemIndexResults = ENTITY.LoadItemIndexResults{
		Result:   0,
		Client:   rq.ParamsBasic.ClientId,
		ItemId:   IIs.SearchItemId,
		Revision: int(cdt.Unix()),
		Cdt:      cdt,
		ExecNo:   strconv.Itoa(idx),
		TTL:      len(*itemsAll),
	}
	fmt.Println(resultOutPut)

	//最終結果の箱に投入
	*IIss = append(*IIss, *IIs)

	ch <- true

	return nil
}

///////////////////////////////////////////////////
/* =========================================== */
//回帰処理
//TagsWordを用いてItemIdを取得していく
/* =========================================== */
func recursiveGetTagsByTagsWord(
	rq *REQ.RequestData,
	latestRev int,
	tagsAll *[]ENTITY.EntityTags,
	itemsAll *[]ENTITY.EntityItems,
	tagsWord string,
	IIs *ENTITY.ItemIdItemIds,
	IIss *[]ENTITY.ItemIdItemIds,
	Depth int,
	ch <-chan bool) error {

	fmt.Println("----------recursiveGetTagsByTagsWord::["+tagsWord, "], Depth:", Depth)

	//DepthCutter 深度レベルを超えたものは検索しない
	if rq.ParamsBasic.DepthCutter.Level > 0 {
		if IIs.Depth >= rq.ParamsBasic.DepthCutter.Level {
			return nil
		}
	}

	//深度をカウント
	IIs.Depth += 1
	Depth += 1

	//先に全取得したTagsの中から、対象となる(検索タグ:tagsWord)entityをすべて取得
	var tagss []ENTITY.EntityTags
	for _, v := range *tagsAll {
		if v.TagsWord == tagsWord {
			var tags ENTITY.EntityTags = ENTITY.EntityTags{
				Revision:            v.Revision,
				ItemId:              v.ItemId,
				TagsWord:            v.TagsWord,
				TagsCatchCopy:       v.TagsCatchCopy,
				TagsStartDate:       v.TagsStartDate,
				TagsEndDateUnixtime: v.TagsStartDateUnixtime,
				TagsEndDate:         v.TagsEndDate,
				TagsIgnoreFlg:       v.TagsIgnoreFlg,
				Cdt:                 v.Cdt,
			}
			tagss = append(tagss, tags)
			IIs.Frequency = IIs.Frequency + 1
		}
	}

	//tagsWord検索した結果、取得できたItemIdで検索
	for _, i := range tagss {

		//すでに確保したItemIdならば、無視
		if !checkerUniqueitemId(*IIs, i.ItemId) {
			continue
		}

		fmt.Println("########Index Count#########", len(IIs.ItemIds))
		//ItemIndexがmax_itemindex_qtyを超えていたら抜ける
		if len(IIs.ItemIds) >= rq.ParamsBasic.MaxItemIndexQty {
			continue
		}

		//ItemIdで検索
		err := recursiveGetTagsByItemId(rq, latestRev, tagsAll, itemsAll, i.ItemId, IIs, IIss, Depth, ch)
		if err != nil {
			//err handle
			return err

		}
	}

	return nil
}

///////////////////////////////////////////////////
/* =========================================== */
//回帰処理
//ItemIdを用いてTagsWordを取得していく
/* =========================================== */
func recursiveGetTagsByItemId(
	rq *REQ.RequestData,
	latestRev int,
	tagsAll *[]ENTITY.EntityTags,
	itemsAll *[]ENTITY.EntityItems,
	itemId string,
	IIs *ENTITY.ItemIdItemIds,
	IIss *[]ENTITY.ItemIdItemIds,
	Depth int,
	ch <-chan bool) error {

	fmt.Println("----------recursiveGetTagsByItemId::["+itemId, "], Depth:", Depth)

	//先に全取得したTagsの中から、対象となる(検索ID:itemId)entityをすべて取得
	var tagsWords []ENTITY.EntityTags
	for _, v := range *tagsAll {
		if v.ItemId == itemId {
			var tags ENTITY.EntityTags = ENTITY.EntityTags{
				Revision:            v.Revision,
				ItemId:              v.ItemId,
				TagsWord:            v.TagsWord,
				TagsCatchCopy:       v.TagsCatchCopy,
				TagsStartDate:       v.TagsStartDate,
				TagsEndDateUnixtime: v.TagsStartDateUnixtime,
				TagsEndDate:         v.TagsEndDate,
				TagsIgnoreFlg:       v.TagsIgnoreFlg,
				Cdt:                 v.Cdt,
			}
			tagsWords = append(tagsWords, tags)
		}
	}
	//TagsWordで昇順ソート
	sort.SliceStable(tagsWords, func(i, j int) bool { return tagsWords[i].TagsWord < tagsWords[j].TagsWord })

	//Searchタグを確保(Searchタグ::TopのItemIdに紐づくTagsWord達)
	if len(IIs.SearchTagsWords) == 0 {
		for _, t := range tagsWords {
			if itemId == t.TagsWord {
				continue
			}
			if checkerUniqueTagsWords(*IIs, t.TagsWord) {
				var st ENTITY.TagsWord = ENTITY.TagsWord{Value: t.TagsWord}
				IIs.SearchTagsWords = append(IIs.SearchTagsWords, st)
			}
		}
	}

	//同じSearchタグを持つ履歴があれば、同じItemIdsを設置して、去る
	for _, iis := range *IIss {
		if reflect.DeepEqual(iis.SearchTagsWords, IIs.SearchTagsWords) {
			IIs.ItemIds = iis.ItemIds
			IIs.Frequency = iis.Frequency
			IIs.Depth = iis.Depth
			*IIss = append(*IIss, *IIs)
			return nil
			//return errors.New(iis.SearchItemId)
		}
	}

	//パラメーターにSearchTagsWordTiedUpがTrueで指定されている場合、
	//検索されたItemIdのTagsWordが、Searchタグに存在するならば取得対象
	if rq.ParamsBasic.SearchTagsWordTiedUp {
		var saveItemIdFlg bool
		for _, st := range IIs.SearchTagsWords {
			for _, t := range tagsWords {
				if st.Value == t.TagsWord {
					saveItemIdFlg = true
					break
				}
			}
			if saveItemIdFlg {
				break
			}
		}
		//取得したTagsのどれもが、Searchタグと合致しない場合、去る
		if !saveItemIdFlg {
			return nil
		}
	}

	//取得したTagsWordをもとに回帰処理
	for _, t := range tagsWords {

		//ItemIdがTagsWordと同じ場合は、無視(TagsWordに検索用として必ずItemIdの値をもたせている)
		if itemId == t.TagsWord {
			continue
		}

		//確保済みのItemIdと重複しない場合、 ItemIdsにインデックスを追加する
		if checkerUniqueitemId(*IIs, t.ItemId) {

			//検索されたItemIdを適用
			var iv ENTITY.ItemId = ENTITY.ItemId{Value: t.ItemId}

			//検索されたItemIdについて、Porpsの値を適用(順番は、パラメーターで指定できる)
			for _, it := range *itemsAll {
				if it.ItemId == itemId {
					for _, o := range rq.ParamsBasic.Orders {
						rtCst := reflect.TypeOf(it)
						rvCst := reflect.ValueOf(it)
						for i := 0; i < rtCst.NumField(); i++ {
							f := rtCst.Field(i)
							v := rvCst.FieldByName(f.Name).Interface()
							if f.Name == o.Name {
								switch v := v.(type) {
								case int:
									iv.Orders = append(iv.Orders, ENTITY.ItemsOrder{Name: o.Name, Value: strconv.Itoa(v)})
								case string:
									iv.Orders = append(iv.Orders, ENTITY.ItemsOrder{Name: o.Name, Value: v})
								default:
								}
								break
							}
						}
					}
					break
				}
			}

			//Itemsを確保
			iv.Depth = Depth
			iv.Frequency = IIs.Frequency
			IIs.ItemIds = append(IIs.ItemIds, iv)
		}

		//TagsWordで検索
		err := recursiveGetTagsByTagsWord(rq, latestRev, tagsAll, itemsAll, t.TagsWord, IIs, IIss, Depth, ch)
		if err != nil {
			//err handle
			fmt.Println(err)
			return err
		}
	}

	return nil
}

// /////////////////////////////////////////////////
// TagsWord::確保している要素に対してUniqかどうかを確認
func checkerUniqueTagsWords(iis ENTITY.ItemIdItemIds, targetTagsWord string) bool {
	judge := true
	for _, vv := range iis.SearchTagsWords {
		if vv.Value == targetTagsWord {
			judge = false
			break
		}
	}
	return judge
}

// /////////////////////////////////////////////////
// ItemId::確保している要素に対してUniqかどうかを確認
func checkerUniqueitemId(iis ENTITY.ItemIdItemIds, targetItemId string) bool {
	judge := true
	for _, vv := range iis.ItemIds {
		if vv.Value == targetItemId {
			judge = false
			break
		}
	}
	return judge
}

///////////////////////////////////////////////////
/* =========================================== */
//Datastoreに、ItemIdのIndex ItemIdsをLoadする
/* =========================================== */
func load2DsItemIdIndex(rq *REQ.RequestData, IIs ENTITY.ItemIdItemIds,
	client *datastore.Client, wg *sync.WaitGroup) error {

	var err error

	defer wg.Done()

	//DSへロードするための箱にデータを詰める(1件)
	cdt := rq.ParamsBasic.Cdt
	var IDX ENTITY.EntityItemIndex = ENTITY.EntityItemIndex{
		Revision:       int(cdt.Unix()),
		ItemId:         IIs.SearchItemId,
		ItemIds:        IIs.ItemIds,
		LatestRevision: rq.ParamsBasic.LatestRevision, //Parameterで指定
		Frequency:      IIs.Frequency,
		Depth:          IIs.Depth,
		Udt:            cdt,
		Cdt:            cdt}

	//PutAll(複数件用)でロードするために、箱さらに箱に詰める
	var ens []ENTITY.EntityItemIndex
	ens = append(ens, IDX)

	//1レコードごとにデータを登録していく
	results, err := q.PutMultiUsingKey(rq, nil, nil, ens, len(ens), client, rq.ParamsBasic.Kind)
	if err != nil {
		fmt.Println("【ERROR】LoadToDs "+rq.ParamsBasic.Kind, ens[0].ItemId, err, len(ens))
		log.Fatal("【ERROR】LoadToDs ", " Kind:"+rq.ParamsBasic.Kind, " ClientId:"+rq.ParamsBasic.ClientId, " SearchItemId:"+IIs.SearchItemId)
		return err
	} else {
		fmt.Println("【SUCCESS】LoadToDs", rq.ParamsBasic.Kind, rq.ParamsBasic.ClientId, ens[0].ItemId)
	}
	fmt.Println(results)

	return nil
}

///////////////////////////////////////////////////
/* =========================================== */
//ロードデータをさらに更新(ItemsIdsにFrequencyとDepthを上書き)
/* =========================================== */
func updateItemIdsByDepthCutter(rq *REQ.RequestData, client *datastore.Client) error {

	var err error

	dsKind := rq.ParamsBasic.Kind //ItemIndex
	cdt := rq.ParamsBasic.Cdt     //今回先に登録されたItemIndexのRevisionに利用したCdt

	//Filterを設定する箱を準備（連想配列名にフィルター名、要素に値）
	args := make(map[string]string)

	//Filterに必要な、Prop名&オペ、値(文字列)を設定
	args["filter_Revision_ieq"] = strconv.Itoa(int(cdt.Unix()))

	//Get parameterとしてFilterを設定(Get parameterを初期化)
	REQ.SettingFilterToGetParamter(rq, &args, true)

	//最新のRevisionより小さいRevision、且つlatestRevisionがtrueのEntityを取得する
	ens, keys, err := q.GetAllByFilter(rq, client, dsKind, nil)
	if err != nil {
		fmt.Println(err)
		return err
	}
	ens1, _ := ens.(*[]ENTITY.EntityItemIndex) //取得したEntityをCast

	//深度の切り捨て指定にもとづいて、ItemIdsの切り捨てを行う
	dCutterOpe, dCutterValues := itemIdsCutter(rq, ens1)
	for i, e1 := range *ens1 {
		var idss []ENTITY.ItemId
		for _, dc := range dCutterValues {
			if dc.ItemId == e1.ItemId {
				for _, ids := range e1.ItemIds {
					if dCutterOpe == "Over" && ids.Depth <= dc.Value {
						//Depthは値が小さいほど高(=Over)
						idss = append(idss, ids)
					} else if dCutterOpe == "Under" && ids.Depth >= dc.Value {
						//Depthは値が大きいほど低(=Under)
						idss = append(idss, ids)
					}
				}
			}
		}
		(*ens1)[i].ItemIds = idss
		(*ens1)[i].ItemIdsQty = len(idss)
	}

	//古いRevのLatestRevisionをFalseに、ChunkしながらUpdate(500件未満づつの更新)
	eis, chunks := ENTITY.CreateChunkBox(dsKind, &ens1)
	for i, c := range chunks.Positions {
		result, err := q.PutMultiUsingKey(rq, nil, keys[c.Start:c.End], eis[i], c.Qty, client, dsKind)
		if err != nil {
			fmt.Println(err, result)
			return err
		}
		fmt.Println("Updated Frequency & Depth values to ItemIds", c.Start, c.End, dsKind)
	}

	return nil
}

///////////////////////////////////////////////////
/* =========================================== */
//指定されたDepthCutterをもとにItemIdsの要素を選別
/* =========================================== */
func itemIdsCutter(rq *REQ.RequestData, nens *[]ENTITY.EntityItemIndex) (string, []ENTITY.DepthCutter) {

	//パラメーターから取得
	dCutterType := rq.ParamsBasic.DepthCutter.Type
	dCutterOpe := rq.ParamsBasic.DepthCutter.Ope
	dCutterRate := float64(rq.ParamsBasic.DepthCutter.Rate)

	//ItemIdごとのItemIds達のDepthの(sum, min, max)から
	//切り捨てる判定値を算出し格納していく
	var dcs []ENTITY.DepthCutter
	for _, ne := range *nens {
		var sum int = 0
		var minD int = 1000000
		var maxD int = 0
		for _, ids := range ne.ItemIds {
			sum = sum + ids.Depth
			if maxD < ids.Depth {
				maxD = ids.Depth
			}
			if minD > ids.Depth {
				minD = ids.Depth
			}
		}
		var val int
		if dCutterType == "Ave" {
			val = sum / len(ne.ItemIds)
		} else {
			val = int(float64((minD + maxD)) * (1.0 - dCutterRate))
		}
		var dc ENTITY.DepthCutter = ENTITY.DepthCutter{
			ItemId: ne.ItemId,
			Value:  val,
		}
		dcs = append(dcs, dc)
	}

	return dCutterOpe, dcs
}

///////////////////////////////////////////////////
/* =========================================== */
//古いロードデータを更新(Udt, LatestRevision=FALSE)
/* =========================================== */
func updateLatestRevision(rq *REQ.RequestData, client *datastore.Client) error {
	err := qix.UpdateLatestRevision(rq, client)
	if err != nil {
		return err
	}
	return nil
}
