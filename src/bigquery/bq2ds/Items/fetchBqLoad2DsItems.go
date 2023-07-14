/*
======================
BigQueryからItemマスターデータを取得する処理
========================
*/
package bigquery

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"unicode/utf8"

	CONFIG "bwing.app/src/config"
	ENTITY "bwing.app/src/datastore/entity"      //共通Query
	QUERY "bwing.app/src/datastore/query"        //共通Query
	IQUERY "bwing.app/src/datastore/query/Items" //Items専用Query
	REQ "bwing.app/src/http/request"
	"github.com/pkg/errors"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/datastore"
	"google.golang.org/api/iterator"
)

// Inerface
type FetchItems struct{}

var q QUERY.Queries
var qi IQUERY.QueryItems

///////////////////////////////////////////////////
/* ===========================================
//取得したItemsデータをDatastoreへロードする
* =========================================== */
func (f FetchItems) FetchBqLoad2Ds(rq *REQ.RequestData, it *bigquery.RowIterator) (int, error) {

	var err error

	/*------------------------------------------------
	事前準備
	//BigQueryから最新データをFetchして、格納
	------------------------------------------------*/

	//Datastore clientを生成(このClinetを使い回す)
	client, err := datastore.NewClient(context.Background(), CONFIG.GetConfig(CONFIG.PROJECT_ID))
	if err != nil {
		fmt.Printf("Error create datastore client.: %v", err)
		return 0, errors.WithStack(err)
	}
	defer client.Close()

	//BigQueryのレコードを格納する箱を準備(カラムごとに配列される)
	var valuess [][]bigquery.Value
	for {
		var values []bigquery.Value

		//これ以上結果が存在しない場合には、iterator.Doneを返して、ループ離脱
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		//エラーハンドル
		if err != nil {
			fmt.Println(err)
			return 0, err
		}
		valuess = append(valuess, values)
	}

	//取得レコード数を確保
	ttl := it.TotalRows

	//結果箱の準備
	var ens []ENTITY.EntityItems

	/*------------------------------------------------
	DSへロード
	//格納したBigQueryの最新データを、DSへロードする
	------------------------------------------------*/

	//BigQueryから取得したデータをItemsの箱に詰める
	for idx, values := range valuess {
		ch := make(chan bool)
		go getItems(rq, int(ttl), idx, values, &ens, client, ch)
		fmt.Println(<-ch) //処理が飛ばないようにくさびを打つ
	}

	//Load数
	loadQty := len(ens)

	//goroutine, sync
	var wg sync.WaitGroup
	wg.Add(loadQty)
	for _, en := range ens {
		go load2DsItems(rq, en, client, &wg)
	}
	wg.Wait()

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
//BigQueryから取得したデータをItemsの箱に詰める
/* =========================================== */
func getItems(rq *REQ.RequestData, ttl, idx int, values []bigquery.Value, ens *[]ENTITY.EntityItems,
	client *datastore.Client, ch chan bool) error {

	defer close(ch)

	//エンティティ作成時間を設定。UNIXタイムに変換した値をRevision値とする
	cdt := rq.ParamsBasic.Cdt
	unixtime := cdt.Unix()

	//ItemIdとDescriptionのUTF8文字数の確認用
	//fmt.Println(int(values[0].(int64)), utf8.RuneCountInString(values[16].(string)))

	//説明文が500文字より大きい場合、DetailとDetail2に分けて登録する
	//var d1, d2 string
	d1, d2, err := divedeDescriptionByLen(500, values[19].(string))
	if err != nil {
		fmt.Println(err)
		return err
	}

	//レコードをエンティティに格納
	e := ENTITY.EntityItems{
		Revision:                int(unixtime),
		ItemId:                  values[0].(string),
		ItemBrandCode:           int(values[1].(int64)),
		ItemBrandStringId:       values[2].(string),
		CodeBrandName:           values[3].(string),
		ItemCategoryCodeL:       int(values[4].(int64)),
		CodeCategoryNameLarge:   values[5].(string),
		ItemCategoryCodeS:       int(values[6].(int64)),
		CodeCategoryNameSmall:   values[7].(string),
		ItemSiteUrl:             values[8].(string),
		ItemTitle:               values[9].(string),
		ItemSex:                 int(values[10].(int64)),
		ItemStartDate:           values[11].(string),
		ItemStartDateUnixTime:   int(values[12].(int64)),
		ItemEndDate:             values[13].(string),
		ItemEndDateUnixTime:     int(values[14].(int64)),
		ItemReleaseDate:         values[15].(string),
		ItemReleaseDateUnixTime: int(values[16].(int64)),
		ItemOrderWeight:         int(values[17].(int64)),
		ItemIgnoreFlg:           values[18].(bool),
		ItemDescriptionDetail:   d1, //values[19]
		ItemDescriptionDetail2:  d2, //values[19]
		//ItemMaterials:       values[20], -->配列で取得
		ItemCatchCopy: values[21].(string),
		Image1stSkuId: values[22].(string),
		//ImageMainUrls        values[23], -->配列で取得
		//ImageSubUrls         values[24], -->配列で取得
		SkuPrice:       int(values[25].(int64)),
		LatestRevision: rq.ParamsBasic.LatestRevision, //Parameterで指定
		Udt:            cdt,
		Cdt:            cdt,
	}

	//Material Arrayを取得
	ms := values[20].([]bigquery.Value)
	for i := 0; i < len(ms); i++ {
		var m ENTITY.ItemMaterial
		m.Material = ms[i].(string)
		e.ItemMaterials = append(e.ItemMaterials, m)
	}

	//ImageMainUrls Arrayを取得
	ims := values[23].([]bigquery.Value)
	for i := 0; i < len(ims); i++ {
		var im ENTITY.ImageMainUrl
		im.Url = ims[i].(string)
		e.ImageMainUrls = append(e.ImageMainUrls, im)
	}

	//ImageSubUrls Arrayを取得
	iss := values[24].([]bigquery.Value)
	for i := 0; i < len(iss); i++ {
		var is ENTITY.ImageSubUrl
		is.Url = iss[i].(string)
		e.ImageSubUrls = append(e.ImageSubUrls, is)
	}

	//Load結果を出力
	var resultOutPut ENTITY.LoadItemsResults = ENTITY.LoadItemsResults{
		Client:   rq.ParamsBasic.ClientId,
		ItemId:   e.ItemId,
		Revision: int(unixtime),
		Cdt:      cdt,
		ExecNo:   strconv.Itoa(idx),
		TTL:      int(ttl),
	}
	fmt.Println(resultOutPut)

	//Entity箱に格納
	*ens = append(*ens, e)

	ch <- true

	return nil
}

///////////////////////////////////////////////////
/* ===========================================
//説明文が指定したUTF8文字数より大きい場合、DetailとDetail2に分けて返却する
//※説明文が1000文字を超えた場合、DS登録でエラーとなるので1000文字で矯正する
* =========================================== */
func divedeDescriptionByLen(splitlen int, desc string) (string, string, error) {
	var d1, d2 string
	var err error

	//UTF8文字数
	l := utf8.RuneCountInString(desc)

	//1000文字に矯正
	max := 1000
	drc := utf8.RuneCountInString(desc)
	if drc > max {
		desc = string([]rune(desc)[:max])
	}

	//指定数以下の場合、d1のみ
	if splitlen >= l {
		d1 = desc
		d2 = ""
		return d1, d2, err
	}

	//d1:500文字と、d2:それ以外
	d1 = string([]rune(desc)[:splitlen])
	d2 = string([]rune(desc)[splitlen:])

	return d1, d2, err
}

///////////////////////////////////////////////////
/* =========================================== */
//Datastoreに、ItemsをLoadする
/* =========================================== */
func load2DsItems(rq *REQ.RequestData, en ENTITY.EntityItems,
	client *datastore.Client, wg *sync.WaitGroup) error {

	defer wg.Done()

	//PutAll(複数件用)でロードするために、箱さらに箱に詰める
	var ens []ENTITY.EntityItems
	ens = append(ens, en)

	//1レコードごとにデータを登録していく
	results, err := q.PutMultiUsingKey(rq, nil, nil, ens, len(ens), client, rq.ParamsBasic.Kind)
	if err != nil {
		fmt.Println("【ERROR】LoadToDs "+rq.ParamsBasic.Kind, ens[0].ItemId, err)
		log.Fatal("【ERROR】LoadToDs ", " Kind:"+rq.ParamsBasic.Kind, " ClientId:"+rq.ParamsBasic.ClientId, " SearchItemId:"+ens[0].ItemId)
		return err
	} else {
		fmt.Println("【SUCCESS】LoadToDs", rq.ParamsBasic.Kind, rq.ParamsBasic.ClientId, ens[0].ItemId)
	}
	fmt.Println(results)

	return nil
}

///////////////////////////////////////////////////
/* =========================================== */
//古いロードデータを更新(Udt, LatestRevision=FALSE)
/* =========================================== */
func updateLatestRevision(rq *REQ.RequestData, client *datastore.Client) error {
	err := qi.UpdateLatestRevision(rq, client)
	if err != nil {
		return err
	}
	return nil
}
