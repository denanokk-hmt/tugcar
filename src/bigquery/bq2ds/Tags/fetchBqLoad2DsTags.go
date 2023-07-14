/*
======================
BigQueryからTagsマスターデータを取得する処理
========================
*/
package bigquery

import (
	"context"
	"fmt"
	"log"
	"strconv"

	CONFIG "bwing.app/src/config"
	ENTITY "bwing.app/src/datastore/entity"      //共通Query
	QUERY "bwing.app/src/datastore/query"        //共通Query
	IQUERY "bwing.app/src/datastore/query/Items" //Items専用Query
	TQUERY "bwing.app/src/datastore/query/Tags"
	REQ "bwing.app/src/http/request"
	"github.com/pkg/errors"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/datastore"
	"google.golang.org/api/iterator"
)

// Inerface
type FetchTags struct{}

var q QUERY.Queries
var qi IQUERY.QueryItems
var qt TQUERY.QueryTags

///////////////////////////////////////////////////
/* ===========================================
//取得したTagsデータをDatastoreへロードする
* =========================================== */
func (foo FetchTags) FetchBqLoad2Ds(rq *REQ.RequestData, it *bigquery.RowIterator) (int, error) {

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

	/*------------------------------------------------
	Itemsデータを取得し、Ansestoreキーのマッピングを生成
	//Itemsのキーを親キーとして、Tagsを登録するため
	------------------------------------------------*/

	//レコードをイテレーターしながら1件づつDatastoreのEntityを作成する
	var ets []ENTITY.EntityTags //Entityを格納する箱を準備
	for _, values := range valuess {

		//Bqから取得したTagsのレコードをEntityの箱に移す
		et, err := setBqValue2TagsEntity(rq, values)
		if err != nil {
			return 0, err
		}
		ets = append(ets, et)
	}

	//ItemsのRevision指定がある場合、取得
	var itemsRevision int
	for _, p := range rq.PostParameter {
		if p.Name == "ItemsRevision" {
			itemsRevision = p.IntValue
			break
		}
	}

	//ItemsのRevision指定がない場合、Latestを1件取得し、TagsのFilterに用いる最新Revisionを取得
	if itemsRevision == 0 {
		ei, _, err := q.GetAllByRevision(rq, 0, 1, client, ENTITY.KIND_ITEMS)
		lei, _ := ei.(*[]ENTITY.EntityItems) //取得したEntityをCast
		if err != nil {
			return 0, err
		} else if len(*lei) == 0 {
			err = errors.New("Items entitis not exits")
			return 0, err
		}
		itemsRevision = (*lei)[0].Revision
	}

	//ItemsデータをDsから取得する
	e, keys, err := q.GetAllByRevision(rq, itemsRevision, -1, client, ENTITY.KIND_ITEMS)
	if err != nil {
		return 0, err
	}
	eia, _ := e.(*[]ENTITY.EntityItems) //取得したEntityをCast

	//Ansestoreキーのマッピングを生成
	pkMaps := make(map[string]*datastore.Key)
	for _, et := range ets {
		for ii, ei := range *eia {
			if et.ItemId == ei.ItemId {
				pkMaps[ei.ItemId] = keys[ii]
				break
			}
		}
	}

	/*------------------------------------------------
	DSへロード
	//格納したBigQueryの最新データを、DSへロードする
	//Itemsのキーを親キーとして、Tagsをグループ登録する
	------------------------------------------------*/

	//上記のDS Clientでは、closeが先に走りErrorを引き起こす。よってここでgo routine用のclientを生成(このClinetを使い回す)
	goRoutineClient, err := datastore.NewClient(context.Background(), CONFIG.GetConfig(CONFIG.PROJECT_ID))
	if err != nil {
		fmt.Printf("Error create datastore client.: %v", err)
		return 0, errors.WithStack(err)
	}

	var loadCount int //レコードをイテレートしながら1件づつDatastoreのEntityを作成する
	for idx, et := range ets {
		//親KeyのValidation
		if pkMaps[et.ItemId] == nil {
			err = errors.New("Items matching PKey not exists. ItemId:" + et.ItemId)
			fmt.Println(err)
			continue
			//log.Fatal(err)
		}
		//Dsへ1件づつロード
		ch := make(chan bool)
		go Load2DsTgas(rq, pkMaps, et, idx, int(ttl), goRoutineClient, ch)
		select {
		case <-ch: //処理が飛ばないようにくさびを打つ
			//fmt.Println(<-ch)
		default:
			//Error
		}
		loadCount++
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

	return loadCount, err
}

///////////////////////////////////////////////////
/* =========================================== */
//Bqから取得したTagsのレコードをEntityの箱に移す
/* =========================================== */
func setBqValue2TagsEntity(rq *REQ.RequestData, values []bigquery.Value) (ENTITY.EntityTags, error) {

	//エンティティ作成時間を設定。UNIXタイムに変換した値をRevision値とする
	cdt := rq.ParamsBasic.Cdt
	unixtime := cdt.Unix()

	//レコードをエンティティに格納
	e := ENTITY.EntityTags{
		Revision:              int(unixtime),
		ItemId:                values[0].(string),
		TagsWord:              values[1].(string),
		TagsCatchCopy:         values[2].(string),
		TagsStartDate:         values[3].(string),
		TagsStartDateUnixtime: int(values[4].(int64)),
		TagsEndDate:           values[5].(string),
		TagsEndDateUnixtime:   int(values[6].(int64)),
		TagsIgnoreFlg:         values[7].(bool),
		LatestRevision:        rq.ParamsBasic.LatestRevision, //Parameterで指定
		Udt:                   cdt,
		Cdt:                   cdt,
	}
	return e, nil
}

///////////////////////////////////////////////////
/* =========================================== */
//Datastoreに、TagsのItems親キーを取得する
//Channelを用いて、*datastore.Keyの上書きを抑える
/* =========================================== */
func Load2DsTgas(rq *REQ.RequestData, pkMaps map[string]*datastore.Key, et ENTITY.EntityTags, idx, ttl int,
	client *datastore.Client, ch chan bool) error {

	//エンティティ作成時間を設定。UNIXタイムに変換した値をRevision値とする
	cdt := rq.ParamsBasic.Cdt
	unixtime := cdt.Unix()

	//親のNameキーをPutMulti向けに格納
	var nks []string
	nks = append(nks, et.ItemId)

	//EntityをPutMulti向けに格納
	var ens []ENTITY.EntityTags
	ens = append(ens, et)

	//Load結果を出力（Putの前だがチャネルを通しているため、それ以降の処理が飛ばされる)
	var resultOutPut ENTITY.LoadTagsResults = ENTITY.LoadTagsResults{
		Client:   rq.ParamsBasic.ClientId,
		Revision: int(unixtime),
		ItemId:   ens[0].ItemId,
		Cdt:      cdt,
		ExecNo:   strconv.Itoa(idx),
		TTL:      int(ttl),
	}
	fmt.Println(resultOutPut)

	//1レコードごとにデータを登録していく
	_, err := q.PutMultiUsingWithChannel(rq, nks, pkMaps, nil, ens, 1, client, rq.ParamsBasic.Kind, ch)
	if err != nil {
		log.Fatal("Put error in Load2DsTgas func().", err, ens)
		return err
	}

	return nil
}

///////////////////////////////////////////////////
/* =========================================== */
//古いロードデータを更新(Udt, LatestRevision=FALSE)
/* =========================================== */
func updateLatestRevision(rq *REQ.RequestData, client *datastore.Client) error {
	err := qt.UpdateLatestRevision(rq, client)
	if err != nil {
		return err
	}
	return nil
}
