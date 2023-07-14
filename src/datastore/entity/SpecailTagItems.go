/* =================================
Datastore
Namespace::WhatYa-Attachment-[client]-[env]
kind::SpecialTagItems
Entiryの構造をここで指定する
* ================================= */
package entity

import (
	"time"

	"cloud.google.com/go/datastore"
)

///////////////////////////////////////////////////
/* ===========================================
Kind::SpecialTagItems
* =========================================== */

//kind::SpecialTagItemsのEntityの箱
type EntitySpecialTagItems struct {
	AttachmentItemRef      string
	Cdt                    time.Time
	Dflg                   bool
	ImageMainUrls          []ImageMainUrl
	ImageSubUrls           []ImageSubUrl
	ItemDescriptionDetail  string
	ItemDescriptionDetail2 string
	ItemOrder              int
	ItemSiteUrl            string
	ItemTitle              string
	SkuPrice               int
	TagID                  string
	Udt                    time.Time
	ItemWords              []string
}

//Search By SpecialTag Response
type EntitySpecialTagItemsResponse struct {
	Requests            SpecialTagItemsRequest
	SpecialTagItemsKeys []int64
	SpecialTagItems     []EntitySpecialTagItems
}
type SpecialTagItemsRequest struct {
	TagID    string
	TagsWord string
}

//Get By SpecialTagItems Response
type EntitySpecialTagItemsGetResponse struct {
	Requests        []SpecialTagItemsGetRequest
	SpecialTagItems []EntitySpecialTagItems
}
type SpecialTagItemsGetRequest struct {
	IDKey int64
	TagID string
	PKey  *datastore.Key
}
