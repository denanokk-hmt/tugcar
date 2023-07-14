/*======================
Datastoreのデータ取得を目的とされたクエリ基本処理
========================*/
package query

import (
	"context"
	"strconv"
	"strings"

	DS_CONFIG "bwing.app/src/datastore/config"
	REQ "bwing.app/src/http/request"

	"cloud.google.com/go/datastore"
)

type GetDsQuery struct{}

///////////////////////////////////////////////////
/* =================================
【GET】
DS_CONFIGのOptionsをセット
Get parameterからLimit, filter, orderを取り出す
定義方法(本クエリ独自)
・Limit	::limit=[int]
・filter::filter_[prop]_[operator]=[Value]
・order	::order_[prop]=[asc/desc]
・key		::key_[name/id]=[Value]
	exp
		~?limit=1
		~?filter_Title_eq=Article1
		~?order_Title=asc
		~?key_id=i, key_name=a
* ================================= */
func NewGetOptions(ig interface{}) *DS_CONFIG.Options {

	//Set defaukt limit
	var op DS_CONFIG.Options = DS_CONFIG.Options{Limit: -1}

	//no get prameter
	if ig == nil {
		//fmt.Println("no get parameter.")
		return &op
	}

	//Get parameter
	p := ig.(*[]REQ.GetParameter)

	for _, v := range *p {
		//get limit
		if v.Name == "limit" {
			op.Limit, _ = strconv.Atoi(v.Value)
			continue
		}
		//get filter, order, key
		n := strings.Split(v.Name, "_")
		if n[0] == "filter" {
			fl := DS_CONFIG.Filter{Name: n[1], Operator: n[2], Value: v.Value}
			op.Filter = append(op.Filter, fl)
		} else if n[0] == "order" {
			od := DS_CONFIG.Order{Name: n[1], Value: v.Value}
			op.Order = append(op.Order, od)
		} else if n[0] == "key" {
			var k DS_CONFIG.Key
			if n[1] == "id" {
				k.Id, _ = strconv.ParseInt(v.Value, 10, 64)
				op.Key = k
			} else if n[1] == "name" {
				k.Name = v.Value
				op.Key = k
			}
		} else {
			continue
		}
	}
	return &op
}

///////////////////////////////////////////////////
/* =================================
【GET】
DS_CONFIGのOptionsをセット
Get parameterからLimit, filter, orderを取り出す
定義方法(本クエリ独自)
・Limit	::limit=[int]
・filter::filter_[prop]_[operator]=[Value]
・order	::order_[prop]=[asc/desc]
・key		::key_[name/id]=[Value]
	exp
		~?limit=1
		~?filter_Title_eq=Article1
		~?order_Title=asc
		~?key_id=i, key_name=a
* ================================= */
func NewGetOptionsMap(ig interface{}) *DS_CONFIG.Options {

	//Set default limit
	var op DS_CONFIG.Options = DS_CONFIG.Options{Limit: -1}

	//no get prameter
	if ig == nil {
		//fmt.Println("no get parameter.")
		return &op
	}

	//Get parameter
	p := ig.([]REQ.GetParameter)

	for _, v := range p {
		//get limit
		if v.Name == "limit" {
			op.Limit, _ = strconv.Atoi(v.Value)
			continue
		}
		//get filter, order, key
		n := strings.Split(v.Name, "_")
		if n[0] == "filter" {
			fl := DS_CONFIG.Filter{Name: n[1], Operator: n[2], Value: v.Value}
			op.Filter = append(op.Filter, fl)
		} else if n[0] == "order" {
			od := DS_CONFIG.Order{Name: n[1], Value: v.Value}
			op.Order = append(op.Order, od)
		} else if n[0] == "key" {
			var k DS_CONFIG.Key
			if n[1] == "id" {
				k.Id, _ = strconv.ParseInt(v.Value, 10, 64)
				op.Key = k
			} else if n[1] == "name" {
				k.Name = v.Value
				op.Key = k
			}
		} else {
			continue
		}
	}
	return &op
}

///////////////////////////////////////////////////
/* =================================
クエリを生成
Limitを追加してクエリを生成し、フィルター、オーダーを登録
* ================================= */
func (q *GetDsQuery) NewGetQueryIncludeClient(cg DS_CONFIG.DsConfig, rq *REQ.RequestData) (context.Context, *datastore.Client, *datastore.Query, error) {

	//Datastore clientを生成
	ctx, client, err := DS_CONFIG.NewClient()
	if err != nil {
		return nil, nil, nil, err
	}

	//Get parameterをインターフェースにセット
	var ig interface{}
	if len(rq.GetParameter) != 0 {
		ig = interface{}(&rq.GetParameter)
		/*-----interfaceを使ったサンプル関数-----
		TypeCheckSmaple(ig) //型のチェックサンプル
		CastSample()        //interface castのサンプル
		-------------------------------------*/
	}

	//クエリオプションを登録
	op := *NewGetOptions(ig)

	//クエリを生成
	qry := datastore.NewQuery(cg.Kind).Limit(op.Limit).Namespace(cg.Ns)

	//フィルターを形成
	qry = DS_CONFIG.NewFilter(qry, op)

	//オーダーを形成
	qry = DS_CONFIG.NewOrder(qry, op)

	return ctx, client, qry, nil
}

///////////////////////////////////////////////////
/* =================================
クエリを生成
Limitを追加してクエリを生成し、フィルター、オーダーを登録
ParentKeyを指定することも可能
* ================================= */
func (q *GetDsQuery) NewGetQuery(cg DS_CONFIG.DsConfig, rq *REQ.RequestData, pKey *datastore.Key, notUseFlg bool) (context.Context, *datastore.Query, error) {

	//Datastore clientを生成
	ctx := context.Background()

	//Get parameterをインターフェースにセット
	var ig interface{}
	if len(rq.GetParameter) != 0 {
		ig = interface{}(&rq.GetParameter)
		/*-----interfaceを使ったサンプル関数-----
		TypeCheckSmaple(ig) //型のチェックサンプル
		CastSample()        //interface castのサンプル
		-------------------------------------*/
	}

	//クエリオプションを登録
	op := *NewGetOptions(ig)

	//クエリを生成
	var qry *datastore.Query
	if pKey == nil {
		qry = datastore.NewQuery(cg.Kind).Limit(op.Limit).Namespace(cg.Ns)
	} else {
		qry = datastore.NewQuery(cg.Kind).Namespace(cg.Ns).Ancestor(pKey)
	}

	//フィルターを形成(ここでフィルタを付けない場合、notUseFlg=false)
	if !notUseFlg {
		qry = DS_CONFIG.NewFilter(qry, op)
	}

	//オーダーを形成
	qry = DS_CONFIG.NewOrder(qry, op)

	return ctx, qry, nil
}

///////////////////////////////////////////////////
/* =================================
SyncMapしたFilterを使ってクエリを生成
Limitを追加してクエリを生成し、フィルター、オーダーを登録
ParentKeyを指定することも可能
* ================================= */
func (q *GetDsQuery) NewGetQuerySyncMap(cg DS_CONFIG.DsConfig, rq *REQ.RequestData, pKey *datastore.Key, notUseFlg bool, mapKey string) (context.Context, *datastore.Query, error) {

	//Datastore clientを生成
	ctx := context.Background()

	//Get parameterをインターフェースにセット
	var ig interface{}
	m, ok := rq.GetParameterSyncMap.Load(mapKey)
	if ok {
		ig = interface{}(m)
	}

	//クエリオプションを登録
	op := *NewGetOptionsMap(ig)

	//クエリを生成
	var qry *datastore.Query
	if pKey == nil {
		qry = datastore.NewQuery(cg.Kind).Limit(op.Limit).Namespace(cg.Ns)
	} else {
		qry = datastore.NewQuery(cg.Kind).Namespace(cg.Ns).Ancestor(pKey)
	}

	//フィルターを形成(ここでフィルタを付けない場合、notUseFlg=false)
	if !notUseFlg {
		qry = DS_CONFIG.NewFilter(qry, op)
	}

	//オーダーを形成
	qry = DS_CONFIG.NewOrder(qry, op)

	return ctx, qry, nil
}
