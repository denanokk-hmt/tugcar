/*
	=================================================================================
	これより以下、サーバー初期構築時につくられたサンプル実装
	=================================================================================
*/

/*======================
Datastoreの
Namespace: WhatYa-Attachment-[client]-[env]
kind: Article
に対して、何らから処理を行なわせ、結果をレスポンスをする
========================*/
package response

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	ENTITY "bwing.app/src/datastore/entity"
	QUERY "bwing.app/src/datastore/query"
	AQUERY "bwing.app/src/datastore/query/Article"
	ERR "bwing.app/src/error"
	REQ "bwing.app/src/http/request"
	"bwing.app/src/http/request/postdata"
)

//Inerface
type ResArticle struct{}

///////////////////////////////////////////////////
/* =========================================== */
//Aritcle kindを挿入
/* =========================================== */
func (res ResArticle) PutUsingKey(w http.ResponseWriter, rq *REQ.RequestData, tran bool) {

	dsKind := rq.ParamsBasic.Kind

	//Methodを取得
	m := rq.Method

	//KeyとEntiryの箱を準備
	ek := ENTITY.EntityKey{}
	e := ENTITY.EntityArticle{}

	//Post dataをEntityの箱へ換装する::kindの定義に合わせて
	for n, v := range rq.PostParameter {
		fmt.Println(n, v)
		if v.Name == "IDKey" {
			ek.ID = v.IDKeyValue
		} else if v.Name == "NameKey" {
			ek.Name = v.NameKeyValue
		} else {
			switch v.Name {
			case "Title":
				e.Title = v.StringValue
			case "Body":
				e.Body = v.StringValue
			case "Content":
				e.Content = v.StringValue
			case "Number":
				e.Number = v.IntValue
			case "PublishedAt":
				e.PublishedAt = v.TimeValue
			}
		}
	}

	//ArticleのentityをEntitiseへ格納
	en := ENTITY.NewEntities(&e)

	//Post Entity
	var q QUERY.Queries
	var err error
	if !tran {
		_, err = q.PutUsingKey(ek, &en, dsKind, m)
	} else {
		_, err = q.PutUsingKeyTran(ek, &en, dsKind, m)
	}

	//Response
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if err != nil {
		ERR.ErrorResponse(w, rq, err, http.StatusBadRequest)
	} else {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode((e))
	}

}

///////////////////////////////////////////////////
/* =========================================== */
//Aritcle kindを一括挿入
/* =========================================== */
func (res ResArticle) PutMultiUsingKeyArticle(w http.ResponseWriter, r *http.Request, rq *REQ.RequestData) {

	//Articleエンティティの箱を準備
	ek := []ENTITY.EntityKey{}
	en := []ENTITY.EntityArticle{}

	//parse json request body
	body, _, err := postdata.ParseJson(r, rq)
	if err == nil {

		//parse json array
		var jsonBody []ENTITY.EntityArticleJson
		if err := json.Unmarshal(body, &jsonBody); err != nil {
			log.Fatal(err)
		}

		//Set entities
		for i, v := range jsonBody {
			fmt.Println(i, v)

			//Set Key
			ka := ENTITY.EntityKey{ID: 0, Name: v.NameKey}
			ek = append(ek, ka)

			//Set Properties
			e := ENTITY.EntityArticle{
				Body:        v.Body,
				Content:     v.Content,
				Title:       v.Title,
				Number:      v.Number,
				PublishedAt: time.Now()}
			en = append(en, e)
		}

		//Post Entities
		var q AQUERY.QueryArticle
		_, err = q.PutMultiUsingKeyArticle(ek, en)
	}

	//Response
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if err != nil {
		ERR.ErrorResponse(w, rq, err, http.StatusBadRequest)
	} else {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode((en))
	}

}
