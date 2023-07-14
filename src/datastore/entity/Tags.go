/* =================================
Datastore
Namespace::WhatYa-Attachment-[client]-[dev]
kind::Tags
Entiryの構造をここで指定する
* ================================= */
package entity

import (
	"time"
)

///////////////////////////////////////////////////
/* ===========================================
Kind::Tags
* =========================================== */

//Entityの箱
type EntityTags struct {
	Revision              int
	ItemId                string
	TagsWord              string
	TagsCatchCopy         string
	TagsStartDate         string
	TagsStartDateUnixtime int
	TagsEndDate           string
	TagsEndDateUnixtime   int
	TagsIgnoreFlg         bool
	LatestRevision        bool
	Udt                   time.Time
	Cdt                   time.Time
}

//Datastore Load result
type LoadTagsResults struct {
	Result   int
	Client   string
	ItemId   string
	Revision int
	Cdt      time.Time
	ExecNo   string
	TTL      int
}

//Load Tags
type LoadTags struct {
	ItemId string
}
