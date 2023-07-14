/* =================================
共有Entityの構造
その他、Entityを扱う共通項目や処理
* ================================= */
package entity

import (
	"fmt"
	"reflect"
	"strings"

	COMMON "bwing.app/src/common" //共通処理
	CONFIG "bwing.app/src/config"
	DS_CONFIG "bwing.app/src/datastore/config"
)

var (
	ATTACHMENT_NS_PREFIX           = "WhatYa-Attachment"
	SEARCH_TAGS_BLOCK_QTY          = 10
	SEARCH_ITEMS_MAX_WORKERS       = 30 //goroutine worker数の上限
	CHUNK_QTY                      = 450
	KIND_ITEMS                     = "Items"
	KIND_SKU                       = "Sku"
	KIND_TAGS                      = "Tags"
	KIND_ITEMINDEX                 = "ItemIndex"
	KIND_IGNOREITEMS               = "IgnoreItems"
	KIND_SPECIALTAG                = "SpecialTag"
	KIND_SPECIALTAGITEMS           = "SpecialTagItems"
	KIND_OPTIONAL_SUFFIX_NOT_ARRAY = "__NotArray" //kind名をイジらずPropsの変異をしたい場合
	PROP_LATESTREVISION            = "LatestRevision"
	PROP_REVISION                  = "Revision"
)

var NsKinds *map[string]nsKind

type nsKind struct {
	ns   string
	kind string
}

var EnKinds *map[string]enKind

type enKind struct {
	entity interface{}
	kind   string
}

type Entities struct {
	Entities interface{}
}

//kind::Keyの箱
type EntityKey struct {
	ID   int64
	Name string
}

//Datastore Load result
type LoadDataResults struct {
	Results interface{}
}

///////////////////////////////////////////////////
//初期化に利用。実態は別途個別に使用させている
//※DatastoreのKindを追加時にKindのEntity(Property)を登録し
//この構造体に登録しておく
///////////////////////////////////////////////////
type EntityProps struct {
	Items                     []EntityItems
	Sku                       []EntitySku
	Tags                      []EntityTags
	ItemIndex                 []EntityItemIndex
	IgnoreItems               []EntityIgnoreItems
	SpecialTag                []EntitySpecialTag
	SpecialTagItems           []EntitySpecialTagItems
	SpecialTagItems__NotArray EntitySpecialTagItems // /get/SpecialTagItemsで利用
}

///////////////////////////////////////////////////
//起動時にGCP NS, Kindを登録する
func init() {

	//Kindに付随するAttachmentのNaspace PrefixとkindをMapしておく
	nk := map[string]nsKind{}
	ek := map[string]enKind{}
	var ep EntityProps
	rtCst := reflect.TypeOf(ep)
	rvCst := reflect.ValueOf(ep)
	for i := 0; i < rtCst.NumField(); i++ {
		f := rtCst.Field(i) // フィールド情報を取得
		v := rvCst.FieldByName(f.Name).Interface()
		nk[f.Name] = nsKind{ns: ATTACHMENT_NS_PREFIX, kind: f.Name}
		ek[f.Name] = enKind{entity: v, kind: f.Name}
	}
	NsKinds = &nk
	fmt.Printf("SET Attachment NS Prefix & Kind mapping of Datastore:%s\n", NsKinds)
}

/* =================================
//返却箱の空の中身
* ================================= */
func NewEntities(p interface{}) Entities {
	var en Entities
	en.Entities = p
	return en
}

/* =================================
//結果箱の空の中身
* ================================= */
func NewLoadDataResults(p interface{}) LoadDataResults {
	var r LoadDataResults
	r.Results = p
	return r
}

///////////////////////////////////////////////////
//共通処理(Interfaceに包む)のために箱の準備とDatastoreのConfigを設定する
//※前提として箱については、src/datastore/entity/[kind名].goファイルに準備が必要
//※[使用するKindを追加するごとに、ソースに追加していく]
func NewConfigEntities(dsKind string) (cg DS_CONFIG.DsConfig, ena interface{}) {

	var en Entities
	var ep EntityProps
	rt := reflect.TypeOf(ep)
	rv := reflect.ValueOf(ep)

	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i) // フィールド情報を取得
		v := rv.FieldByName(f.Name).Interface()
		if f.Name == dsKind {
			switch v.(type) {
			case []EntityItems:
				ev := v.([]EntityItems)
				en = NewEntities(&ev)
			case []EntitySku:
				ev := v.([]EntitySku)
				en = NewEntities(&ev)
			case []EntityTags:
				ev := v.([]EntityTags)
				en = NewEntities(&ev)
			case []EntityItemIndex:
				ev := v.([]EntityItemIndex)
				en = NewEntities(&ev)
			case []EntityIgnoreItems:
				ev := v.([]EntityIgnoreItems)
				en = NewEntities(&ev)
			case []EntitySpecialTag:
				ev := v.([]EntitySpecialTag)
				en = NewEntities(&ev)
			case []EntitySpecialTagItems:
				ev := v.([]EntitySpecialTagItems)
				en = NewEntities(&ev)
			case EntitySpecialTagItems:
				ev := v.(EntitySpecialTagItems)
				en = NewEntities(&ev)
			}
			break
		}
	}

	//Entitiesをkind名で連想配列にMap
	ena = map[string]Entities{dsKind: en}

	//DSのConfigを設定
	cg = SetDsConfig(dsKind)

	return cg, ena
}

///////////////////////////////////////////////////
//DatastoreのGCP ProjectId, Namespace, Kindを箱に入れて戻す
func SetDsConfig(dsKind string) DS_CONFIG.DsConfig {

	//[Kind]_[Optional]の場合、Optional文字列(__Suffix)を削除
	dsKind = strings.Split(dsKind, "__")[0]

	//Kind名からNSを取得
	var ns string
	for k, v := range *NsKinds {
		if k == dsKind {
			ns = v.ns
			break
		}
	}

	//箱詰め
	var cg DS_CONFIG.DsConfig = DS_CONFIG.DsConfig{
		ProjectId: CONFIG.GetConfig(CONFIG.PROJECT_ID),
		Ns:        ns,
		Kind:      dsKind}

	return cg
}

///////////////////////////////////////////////////
//Chunkを使いながら更新する処理に対して、
//入れ物の箱をChunkごとに入れ分けて返却する
//※[必要に応じて、ソースに追加していく]
func CreateChunkBox(dsKind string, ens interface{}) ([]interface{}, COMMON.Chunks) {

	var eis []interface{}
	var chunks COMMON.Chunks
	switch dsKind {
	case KIND_ITEMS:
		ens1, _ := ens.(**[]EntityItems)
		chunks = COMMON.ChunkCalculator2(len(**ens1), CHUNK_QTY)
		for _, c := range chunks.Positions {
			ei := interface{}((**ens1)[c.Start:c.End])
			eis = append(eis, ei)
		}
	case KIND_SKU:
		ens1, _ := ens.(**[]EntitySku)
		chunks = COMMON.ChunkCalculator2(len(**ens1), CHUNK_QTY)
		for _, c := range chunks.Positions {
			ei := interface{}((**ens1)[c.Start:c.End])
			eis = append(eis, ei)
		}
	case KIND_TAGS:
		ens1, _ := ens.(**[]EntityTags)
		chunks = COMMON.ChunkCalculator2(len(**ens1), CHUNK_QTY)
		for _, c := range chunks.Positions {
			ei := interface{}((**ens1)[c.Start:c.End])
			eis = append(eis, ei)
		}
	case KIND_ITEMINDEX:
		ens1, _ := ens.(**[]EntityItemIndex)
		chunks = COMMON.ChunkCalculator2(len(**ens1), CHUNK_QTY)
		for _, c := range chunks.Positions {
			ei := interface{}((**ens1)[c.Start:c.End])
			eis = append(eis, ei)
		}
	}
	return eis, chunks
}

///////////////////////////////////////////////////
//Get Kind name
func GetKindName(p []string) (string, bool) {

	//Get kind name from url path
	lp := len(p)
	k := p[lp-1]

	//validation kind name
	var b = ValidationKind(k)
	if b {
		return k, true
	} else {
		return "[NoDS]", false
	}
}

///////////////////////////////////////////////////
//Validaition kind name
func ValidationKind(dsKind string) bool {
	var b = false
	for i, _ := range *NsKinds {
		if dsKind == i {
			b = true
			break
		}
	}
	return b
}
