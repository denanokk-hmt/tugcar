/*======================
Datastoreのデータ作成を目的とされたクエリ基本処理
========================*/
package query

import (
	"context"
	"strings"

	DS_CONFIG "bwing.app/src/datastore/config"
	REQ "bwing.app/src/http/request"

	"cloud.google.com/go/datastore"
)

type PostDsQuery struct{}

///////////////////////////////////////////////////
/* =================================
【Post】
DS_CONFIGのOptionsをセット
* ================================= */
func NewPostOptions(ig interface{}) *DS_CONFIG.Options {

	//Set defaukt limit
	var op DS_CONFIG.Options = DS_CONFIG.Options{Limit: -1}

	//no get prameter
	if ig == nil {
		//fmt.Println("no get parameter.")
		return &op
	}

	//Post parameter
	p := ig.(*[]REQ.PostParameter)

	for _, v := range *p {
		switch v.Name {
		case "Query_Limit":
			op.Limit = v.IntValue
		case "Query_Orders":
			orders := v.StringArray
			for _, o := range orders {
				sp := strings.Split(o.Value, ":")
				od := DS_CONFIG.Order{Name: sp[0], Value: sp[1]}
				op.Order = append(op.Order, od)
			}
		case "Query_Tags":
			tags := v.StringArray
			for _, t := range tags {
				fl := DS_CONFIG.Filter{Name: "TagsWord", Operator: "seq", Value: t.Value}
				op.Filter = append(op.Filter, fl)
			}
		//case "Revision":
		//	fl := DS_CONFIG.Filter{Name: "Revision", Operator: "iceq", Value: strconv.Itoa(v.IntValue)}
		//	op.Filter = append(op.Filter, fl)
		default:
		}
	}
	return &op
}

///////////////////////////////////////////////////
/* =================================
【Post】
クエリを生成
* ================================= */
func (q PostDsQuery) NewPostQuery(cg DS_CONFIG.DsConfig, rq *REQ.RequestData, pKey *datastore.Key, notUseFlg bool) (context.Context, *datastore.Client, *datastore.Query, error) {

	//Datastore clientを生成
	ctx, client, err := DS_CONFIG.NewClient()
	if err != nil {
		return nil, nil, nil, err
	}

	//POST parameterをインターフェースにセット
	var ig interface{}
	if len(rq.PostParameter) != 0 {
		ig = interface{}(&rq.PostParameter)
		/*-----interfaceを使ったサンプル関数-----
		TypeCheckSmaple(ig) //型のチェックサンプル
		CastSample()        //interface castのサンプル
		-------------------------------------*/
	}

	//クエリオプションを登録
	op := *NewPostOptions(ig)

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

	return ctx, client, qry, nil
}

///////////////////////////////////////////////////
/* =================================
【Post】
クエリを生成
datastore.Clientは共通→引数
* ================================= */
func (q PostDsQuery) NewPostQueryIncludeClient(cg DS_CONFIG.DsConfig, rq *REQ.RequestData, pKey *datastore.Key, notUseFlg bool, client *datastore.Client) (context.Context, *datastore.Client, *datastore.Query, error) {

	//Datastore clientを生成
	ctx := context.Background()

	//POST parameterをインターフェースにセット
	var ig interface{}
	if len(rq.PostParameter) != 0 {
		ig = interface{}(&rq.PostParameter)
		/*-----interfaceを使ったサンプル関数-----
		TypeCheckSmaple(ig) //型のチェックサンプル
		CastSample()        //interface castのサンプル
		-------------------------------------*/
	}

	//クエリオプションを登録
	op := *NewPostOptions(ig)

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

	return ctx, client, qry, nil
}

///////////////////////////////////////////////////
/* =================================
【Post】
トランザクションクエリを生成
*idかnameかのどちらかしか使えない
* ================================= */
func (q PostDsQuery) NewPostQueryTran(cg DS_CONFIG.DsConfig, rq *REQ.RequestData) (context.Context, *datastore.Client, *datastore.Query, error) {

	//Datastore clientを生成
	ctx, client, err := DS_CONFIG.NewClient()
	if err != nil {
		return nil, nil, nil, err
	}

	//Get parameterをインターフェースにセット
	var ig interface{}
	if len(rq.PostParameter) != 0 {
		ig = interface{}(&rq.PostParameter)
		/*-----interfaceを使ったサンプル関数-----
		TypeCheckSmaple(ig) //型のチェックサンプル
		CastSample()        //interface castのサンプル
		-------------------------------------*/
	}

	//クエリオプションを登録
	op := *NewPostOptions(ig)

	//クエリを生成
	qry := datastore.NewQuery(cg.Kind).Limit(op.Limit).Namespace(cg.Ns)

	//フィルターを形成
	qry = DS_CONFIG.NewFilter(qry, op)

	//オーダーを形成
	qry = DS_CONFIG.NewOrder(qry, op)

	return ctx, client, qry, nil
}
