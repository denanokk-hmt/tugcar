/* =================================
Datastore
Namespace::WhatYa-Attachment-[client]-[env]
kind::IgnoreItems
Entiryの構造をここで指定する
* ================================= */
package entity

import (
	"time"
)

///////////////////////////////////////////////////
/* ===========================================
Kind::IgnoreItems
* =========================================== */

//登録用
//kind::IgnoreItemsのEntityの箱
type EntityIgnoreItems struct {
	By                  string
	Id                  string
	IgnoreSince         string
	IgnoreSinceUnixtime int
	IgnoreUntil         string
	IgnoreUntilUnixtime int
	IgnoreDflg          bool
	Udt                 time.Time
	Cdt                 time.Time
}
