/*
======================
HTTPサーバーのRouting
========================
*/
package http

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	RC "bwing.app/src/http/request/datastore"
	RC2 "bwing.app/src/http/request/gcs"
)

///////////////////////////////////////////////////
/* =========================================== */
//HTTP Request Router
/* =========================================== */
func HandleRequests() {

	port, _ := strconv.Atoi(os.Args[1])
	fmt.Println(port)

	//Root Routing for inner health check
	http.HandleFunc("/", root)
	http.HandleFunc("/hmt/hello", hello)

	/*
		Datastore
			・load処理フロー(bqから取得-->Datastoreへ登録)
			・search, get処理フロー(Datastoreから取得-->レスポンス)
			・厳密なRestAPIの方式には従わない(基本的にPOSTメソッドで登録・取得を行なっている)
			・http/request/datastore/~ -->http/response/datastore/~ -->datastore/query
	*/
	rc := RC.NewRequests()

	//AttachmentのLoad
	http.HandleFunc("/hmt/attachment/load/", rc.PostDsWithJsonSwitch)

	//AttachmentのUpdate
	http.HandleFunc("/hmt/attachment/update/", rc.PostDsWithJsonSwitch)

	//AttachmentのSearch
	http.HandleFunc("/hmt/attachment/search/", rc.PostDsWithJsonSwitch)

	//Attachmentのget
	http.HandleFunc("/hmt/attachment/get/", rc.PostDsWithJsonSwitch)

	//Attachmentのdelete
	http.HandleFunc("/hmt/attachment/delete/", rc.DeleteDsSwitch)

	//サーバー初期構築でサンプル実装したもの
	//パスの先頭はMethodで処理をスイッチさせるようにinterface
	http.HandleFunc("/get/", rc.GetDsSwitch)
	http.HandleFunc("/post/", rc.PostDsSwitch)
	http.HandleFunc("/put/", rc.PutDsSwitch)
	http.HandleFunc("/delete/", rc.DeleteDsSwitch)

	/*
		GCS-->BQ
			※flight-logbookへ切り出すので、この点の開発は中途半端(Refなし)に終わります。
			→いずれソースコードを削除する
			・load処理フロー(GCSから取得-->Bqへロード)
			・厳密なRestAPIの方式には従わない(基本的にPOSTメソッドで登録・取得を行なっている)
			・http/request/gcs/~ -->http/response/datastore/~ -->datastore/query
	*/
	rc2 := RC2.NewRequests()

	//ChainedTagsLoggingのLoad
	http.HandleFunc("/hmt/gcs/load/", rc2.PostBqWithJsonSwitch)

	//Attachiment画像判定
	http.HandleFunc("/hmt/bq/load/", rc2.PostBqWithJsonSwitch)

	//Listen
	p := ":" + strconv.Itoa(port)
	fmt.Println("GoGo Tugcar!!" + p)
	log.Fatal(http.ListenAndServe(p, nil))

}

///////////////////////////////////////////////////
/* =========================================== */
//Root response for INNER Health check
/* =========================================== */
func root(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "HELLO ROOT!!")
	fmt.Println("Endpoint Hit: root")
}

///////////////////////////////////////////////////
/* =========================================== */
//Appli Root response for Service
/* =========================================== */
func hello(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Tugcar is Running!! by Bwing project")
}
