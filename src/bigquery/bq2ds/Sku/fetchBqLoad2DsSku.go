/*
======================
BigQueryからSkuマスターデータを取得する処理
========================
*/
package bigquery

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"

	CONFIG "bwing.app/src/config"
	ENTITY "bwing.app/src/datastore/entity"
	QUERY "bwing.app/src/datastore/query" //共通Query
	SQUERY "bwing.app/src/datastore/query/Sku"
	REQ "bwing.app/src/http/request"
	"github.com/pkg/errors"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/datastore"
	"google.golang.org/api/iterator"
)

// Inerface
type FetchSku struct{}

var q QUERY.Queries
var qs SQUERY.QuerySku

///////////////////////////////////////////////////
/* ===========================================
//取得したSkuデータをDatastoreへロードする
* =========================================== */
func (f FetchSku) FetchBqLoad2Ds(rq *REQ.RequestData, it *bigquery.RowIterator) (int, error) {

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
	var ens []ENTITY.EntitySku

	/*------------------------------------------------
	DSへロード
	//格納したBigQueryの最新データを、DSへロードする
	------------------------------------------------*/

	//BigQueryから取得したデータをSkuの箱に詰める
	for idx, values := range valuess {
		//Dsへロード
		ch := make(chan bool)
		go getSku(rq, int(ttl), idx, values, &ens, client, ch)
		fmt.Println(<-ch) //処理が飛ばないようにくさびを打つ
	}

	//Load数
	loadQty := len(ens)

	//goroutine, sync
	var wg sync.WaitGroup
	wg.Add(int(loadQty))
	for _, en := range ens {
		go load2DsSku(rq, en, &wg, client)
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
//BigQueryから取得したデータをSkuの箱に詰める
/* =========================================== */
func getSku(rq *REQ.RequestData, ttl, idx int, values []bigquery.Value, ens *[]ENTITY.EntitySku,
	client *datastore.Client, ch chan bool) error {

	defer close(ch)

	//Extractでわざと紐付かせないデータを用いた場合の考慮
	if values[0] == nil {
		return nil
	}

	//エンティティ作成時間を設定。UNIXタイムに変換した値をRevision値とする
	cdt := rq.ParamsBasic.Cdt
	unixtime := cdt.Unix()

	//レコードをエンティティに格納
	e := ENTITY.EntitySku{
		Revision:                     int(unixtime),
		SkuId:                        values[0].(string),
		ItemId:                       values[1].(string),
		SkuPrice:                     int(values[2].(int64)),
		SkuPrice_tax:                 int(values[3].(int64)),
		SkuDiscountPrice:             int(values[4].(int64)),
		SkuDiscountPrice_tax:         int(values[5].(int64)),
		SkuDiscountStartDate:         values[6].(string),
		SkuDiscountStartDateUnixtime: int(values[7].(int64)),
		SkuDiscountEndDate:           values[8].(string),
		SkuDiscountEndDateUnixtime:   int(values[9].(int64)),
		SkuIgnoreFlg:                 values[10].(bool),
		//SkuDetails                   values[11], -->配列で取得
		SkuStockQty: int(values[12].(int64)),
		//ImageSubUrls                 values[13], -->配列で取得
		LatestRevision: rq.ParamsBasic.LatestRevision, //Parameterで指定
		Udt:            cdt,
		Cdt:            cdt,
	}

	//SkuDetails Arrayを取得
	ds := values[11].([]bigquery.Value)
	for i := 0; i < len(ds); i++ {
		var d ENTITY.SkuDetail
		d.Value = ds[i].(string)
		e.SkuDetails = append(e.SkuDetails, d)
	}

	//SkuImageSubUrls Arrayを取得
	iss := values[13].([]bigquery.Value)
	for i := 0; i < len(iss); i++ {
		var is ENTITY.SkuImageSubUrl
		is.Url = iss[i].(string)
		e.ImageSubUrls = append(e.ImageSubUrls, is)
	}

	//Load結果を出力
	var resultOutPut ENTITY.LoadSkuResults = ENTITY.LoadSkuResults{
		Client:   rq.ParamsBasic.ClientId,
		SkuId:    e.SkuId,
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
/* =========================================== */
//Datastoreに、SkuをLoadする
/* =========================================== */
func load2DsSku(rq *REQ.RequestData, en ENTITY.EntitySku,
	wg *sync.WaitGroup, client *datastore.Client) error {

	defer wg.Done()

	var ens []ENTITY.EntitySku
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
	err := qs.UpdateLatestRevision(rq, client)
	if err != nil {
		return err
	}
	return nil
}
