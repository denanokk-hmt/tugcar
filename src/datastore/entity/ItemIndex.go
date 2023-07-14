/* =================================
Datastore
Namespace::WhatYa-Attachment-[client]-[env]
kind::ItemIndex
Entiryの構造をここで指定する
* ================================= */
package entity

import (
	"time"
)

///////////////////////////////////////////////////
/* ===========================================
Kind::ItemIndex
* =========================================== */

//Entityの箱
type EntityItemIndex struct {
	Revision       int
	ItemId         string
	ItemIdsQty     int
	ItemIds        []ItemId
	Frequency      int
	Depth          int
	LatestRevision bool
	Udt            time.Time
	Cdt            time.Time
}

//深度でItemIdsを調整する値
type DepthCutter struct {
	ItemId string
	Value  int
}

//Load ItemIndex
type LoadItemIndex struct {
	ItemId string
}

//Datastore Load result
type LoadItemIndexResults struct {
	Result   int
	Client   string
	ItemId   string
	Revision int
	Cdt      time.Time
	ExecNo   string
	TTL      int
}

//For Tags transform --> ItemIndex登録のための箱
type ItemIdItemIds struct {
	SearchItemId    string
	SearchTagsWords []TagsWord
	SearchOrders    []ItemsOrder
	ItemIds         []ItemId
	Frequency       int
	Depth           int
}
type TagsWord struct {
	Value string
}
type ItemId struct {
	Value     string
	Frequency int
	Depth     int
	Orders    []ItemsOrder
}
type ItemsOrder struct {
	Name  string
	Value string
}

//Search Itemsで使用
type ItemIds struct {
	Value     string
	Frequency int
	Depth     int
}
