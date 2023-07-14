/*======================
Datastore回りの
クエリ等の基本設定
========================*/
package config

import (
	"context"
	"fmt"
	"strconv"
	"time"

	CONFIG "bwing.app/src/config"

	"cloud.google.com/go/datastore"
	"github.com/pkg/errors"
)

//Datastore基本3要素(必須)
type DsConfig struct {
	ProjectId string
	Ns        string
	Kind      string
}

//Confg公開用の箱
var Configs DsConfig

//クエリ全般に利用する値等の箱
type Options struct {
	Limit  int
	Filter []Filter
	Order  []Order
	Key    Key
}

//フィルター用の箱
type Filter struct {
	Name     string //property name
	Operator string
	Value    string
	IntValue int
}

//順序の箱
type Order struct {
	Name  string //property name
	Value string //desc or asc
}

//キーの箱
type Key struct {
	Id   int64
	Name string
	PKey *datastore.Key
}

//Transactionの箱
type Tran struct {
	Run bool
}

//Transactionの登録
func NewTran(tran bool) Tran {
	var tx Tran = Tran{
		Run: true}
	return tx
}

///////////////////////////////////////////////////
/* =================================
//Datastore Clientの登録, Config(基本3要素)の設定、コンテキストの設定
* ================================= */
func NewClient() (context.Context, *datastore.Client, error) {

	//プロジェクトIDを指定
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, CONFIG.GetConfig(CONFIG.PROJECT_ID))
	if err != nil {
		fmt.Printf("Error create datastore client.: %v", err)
		return ctx, client, errors.WithStack(err)
	}
	return ctx, client, nil
}

///////////////////////////////////////////////////
/* =================================
フィルターを生成
※複合インデックスはここでは考慮しない
※複数Filterを用いる場合は、複合indexの追加前提での利用
(以下独自定義)
・eq::文字列での一致を意味する
・seq::文字列での一致を意味する
・iceq::文字列を数値に変換し、数値での一致を意味する
・ieq::数値での一致を意味する
・gt, ge, le, lt::数値での比較を意味する
・日付型は、epoctimeにして利用する
* ================================= */
func NewFilter(qry *datastore.Query, op Options) *datastore.Query {
	fl := op.Filter
	for _, v := range fl {
		switch v.Operator {
		case "seq", "eq": //文字列一致
			qry = qry.Filter(v.Name+" =", v.Value)
		case "beq": //Bool一致
			var b bool
			b, _ = strconv.ParseBool(v.Value)
			qry = qry.Filter(v.Name+" =", b)
		case "sne", "ne": //文字列不一致
			qry = qry.Filter(v.Name+" !=", v.Value)
		case "bne": //Bool不一致
			var b bool
			b, _ = strconv.ParseBool(v.Value)
			qry = qry.Filter(v.Name+" !=", b)
		default: //数値比較
			var n int
			n, _ = strconv.Atoi(v.Value) //GetパラメーターはValueをstringで受けるので、数値変換必須
			switch v.Operator {
			case "ieq", "iceq": //数値一致
				qry = qry.Filter(v.Name+" =", n)
			case "ine", "icne": //数値不一致
				qry = qry.Filter(v.Name+" !=", n)
			case "beq": //bool一致
				qry = qry.Filter(v.Name+" =", n)
			case "bne": //bool不一致
				qry = qry.Filter(v.Name+" !=", n)
			case "gt": //数値大なり
				qry = qry.Filter(v.Name+" >", n)
			case "ge": //数値以上
				qry = qry.Filter(v.Name+" >=", n)
			case "le": //数値以下
				qry = qry.Filter(v.Name+" <=", n)
			case "lt": //数値小なり
				qry = qry.Filter(v.Name+" <", n)
			default:
				//以下、一応日付型を書いているが検証していない
				switch v.Operator {
				case "deq", "dge", "dgt", "dle", "dlt":
					d, _ := time.Parse("2006-01-02T15:04:05.000000Z", v.Value)
					switch v.Operator {
					case "deq": //一致
						qry = qry.Filter(v.Name+" =", d)
					case "dne": //不一致
						qry = qry.Filter(v.Name+" !=", d)
					case "dgt": //大なり
						qry = qry.Filter(v.Name+" >", d)
					case "dge": //以上
						qry = qry.Filter(v.Name+" >=", d)
					case "dle": //以下
						qry = qry.Filter(v.Name+" <=", d)
					case "dlt": //小なり
						qry = qry.Filter(v.Name+" <", d)
					}
				}
			}
		}
	}
	return qry
}

///////////////////////////////////////////////////
/* =================================
オーダーを生成
* ================================= */
func NewOrder(qry *datastore.Query, op Options) *datastore.Query {
	or := op.Order
	for _, v := range or {
		switch v.Value {
		case "asc": //昇順
			qry = qry.Order(v.Name)
		case "desc": //降順
			qry = qry.Order("-" + v.Name)
		default:
		}
	}
	return qry
}

///////////////////////////////////////////////////
/* =================================
Keyを生成
* ================================= */
func NewIDKey(kind string, id int64, pk *datastore.Key) *datastore.Key {
	key := datastore.IDKey(kind, id, pk)
	return key
}
func NewNameKey(kind string, name string, pk *datastore.Key) *datastore.Key {
	key := datastore.NameKey(kind, name, pk)
	return key
}
