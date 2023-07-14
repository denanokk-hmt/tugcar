/* =================================
Datastore
Namespace::WhatYa-Attachment-[client]-[env]
kind::Sku
Entiryの構造をここで指定する
* ================================= */
package entity

import (
	"time"
)

///////////////////////////////////////////////////
/* ===========================================
Kind::Sku
* =========================================== */

//Entityの箱
type EntitySku struct {
	Revision                     int
	SkuId                        string
	ItemId                       string
	SkuPrice                     int
	SkuPrice_tax                 int
	SkuDiscountPrice             int
	SkuDiscountPrice_tax         int
	SkuDiscountStartDate         string
	SkuDiscountStartDateUnixtime int
	SkuDiscountEndDate           string
	SkuDiscountEndDateUnixtime   int
	SkuDetails                   []SkuDetail
	SkuStockQty                  int
	SkuIgnoreFlg                 bool
	ImageSubUrls                 []SkuImageSubUrl
	LatestRevision               bool
	Udt                          time.Time
	Cdt                          time.Time
}
type SkuDetail struct {
	Value string
}
type SkuImageSubUrl struct {
	Url string
}

//Datastore Load result
type LoadSkuResults struct {
	Result   int
	Client   string
	SkuId    string
	Revision int
	Cdt      time.Time
	ExecNo   string
	TTL      int
}

//Load Sku
type LoadSku struct {
	SkuId string
}

//Search Items Response
type EntitySkusResponse struct {
	Requests SkuRequest
	Sku      []EntitySku
}
type SkuRequest struct {
	Items string
}
