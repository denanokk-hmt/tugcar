/* =================================
Datastore
Namespace::WhatYa-Attachment-[client]-[env]
kind::SpecialTag
Entiryの構造をここで指定する
* ================================= */
package entity

import (
	"time"
)

///////////////////////////////////////////////////
/* ===========================================
Kind::SpecialTag
* =========================================== */

//登録用
//kind::SpecialTagのEntityの箱
type EntitySpecialTag struct {
	Cdt       time.Time
	Committer string
	Dflg      bool
	TagID     string
	TagsWord  string
	Udt       time.Time
}

//Search SpecialTag Response
type EntitySpecialTagResponse struct {
	Requests             SpecialTagRequest
	SpecialTag           []EntitySpecialTag
	ChildSpecialTagItems []EntitySpecialTagItemsResponse
}
type SpecialTagRequest struct {
	TagsWord []string
}
