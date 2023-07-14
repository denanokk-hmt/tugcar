/* =================================
Datastore
Namespace::WhatYa-Attachment-[client]-[env]
kind::Items
Entiryの構造をここで指定する
* ================================= */
package entity

import (
	"time"
)

///////////////////////////////////////////////////
/* ===========================================
Kind::Items
* =========================================== */

//Entityの箱
type EntityItems struct {
	Revision                    int
	ItemId                      string
	ItemCategoryCodeL           int
	ItemCategoryCodeLSearchCalc int
	ItemCategoryCodeS           int
	ItemCategoryCodeSSearchCalc int
	ItemBrandCode               int
	ItemBrandStringId           string
	CodeBrandName               string
	CodeCategoryNameLarge       string
	CodeCategoryNameSmall       string
	ItemSiteUrl                 string
	ItemTitle                   string
	ItemSex                     int
	ItemStartDate               string
	ItemStartDateUnixTime       int
	ItemEndDate                 string
	ItemEndDateUnixTime         int
	ItemReleaseDate             string
	ItemReleaseDateUnixTime     int
	ItemOrderWeight             int
	ItemIgnoreFlg               bool
	ItemDescriptionDetail       string
	ItemDescriptionDetail2      string
	ItemMaterials               []ItemMaterial
	ItemCatchCopy               string
	Image1stSkuId               string
	ImageMainUrls               []ImageMainUrl
	ImageSubUrls                []ImageSubUrl
	SkuPrice                    int
	LatestRevision              bool
	Udt                         time.Time
	Cdt                         time.Time
	Frequency                   int //Items Searchで利用
	Depth                       int //Items Searchで利用
	ItemWords                   []string
}
type ItemMaterial struct {
	Material string
}
type ImageMainUrl struct {
	Url string
}
type ImageSubUrl struct {
	Url string
}

//Datastore Load result
type LoadItemsResults struct {
	Result   int
	Client   string
	ItemId   string
	Revision int
	Cdt      time.Time
	ExecNo   string
	TTL      int
}

//Load Items
type LoadItems struct {
	ItemId string
}

//Search Items
type EntityItemss struct {
	Entity []EntityItems
}

//Search Items Response
type EntityItemssResponse struct {
	Requests    TagsRequest
	SearchItems EntityItems
	Orders      []Order
	Items       []EntityItems
}
type TagsRequest struct {
	TagsWords []TagsWord
}
type Order struct {
	Name  string
	Value string
}

//Get Items Response
type EntityItemIdsResponse struct {
	Requests   ItemIdsRequest
	GetItemIds []string
	Orders     []Order
	Items      []EntityItems
}
type ItemIdsRequest struct {
	ItemIds []string
}
