/* =================================
Datastore
Namespace::Teter
kind::Article
Entiryの構造をここで指定
* ================================= */
package entity

import (
	"time"
)

///////////////////////////////////////////////////
/* ===========================================
Kind::Article
* =========================================== */

//登録用
//kind::ArticleのEntityの箱
type EntityArticle struct {
	Title       string
	Body        string `datastore:",noindex"`
	Content     string
	Number      int
	PublishedAt time.Time
}

//検索用
//kind::ArticleのEntityの箱
type EntityArticleWithKey struct {
	Key         KeyArticle
	Title       string
	Body        string `datastore:",noindex"`
	Content     string
	Number      int
	PublishedAt time.Time
}

//JSON投入用
//kind::ArticleのEntityの箱
type EntityArticleJson struct {
	NameKey     string    `json:"Namekey"`
	Title       string    `json:"Title"`
	Body        string    `json:"Body"`
	Content     string    `json:"Content"`
	Number      int       `json:"Number"`
	PublishedAt time.Time `json:"PublishedAt"`
}

//kind::ArticleのKeyの箱
type KeyArticle struct {
	ID   int64
	Name string
}
