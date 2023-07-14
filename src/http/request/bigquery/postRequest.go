/*
======================
Datastoreのデータ作成を目的とされたリクエストの受け
※データ取得も利用
========================
*/
package request

import (
	"net/http"
	"strings"
	"time"

	ERR "bwing.app/src/error"
	REQ "bwing.app/src/http/request"
	POSTD "bwing.app/src/http/request/postdata"
	RES "bwing.app/src/http/response"
	LOG "bwing.app/src/log"
	"github.com/pkg/errors"
)

///////////////////////////////////////////////////
/* =========================================== */
//POST  Method request
/* =========================================== */
func PostBqWithJson(w http.ResponseWriter, r *http.Request, rq REQ.RequestData) {

	////////////***共通前準備***////////////

	rq.ParamsBasic.Cdt = time.Now() //処理開始時間
	REQ.NewActionName(r, &rq)       //URLPathからDSのAction名を取得
	REQ.NewLastName(r, &rq)         //URLPathからDSのKind名を取得

	//Jsonデータを取得
	err := POSTD.ParseAttachmentLoadJsonData(w, r, &rq)
	//err := POSTD.ParseJsonData(r, &rq)
	if err != nil {
		ERR.ErrorResponse(w, &rq, err, http.StatusBadRequest)
		return
	}

	//Paramsの型をDSに合わせて変換
	REQ.ConvertTypeParams(&rq)
	//リクエストをロギングする
	go LOG.ApiRequestLogging(&rq, LOG.INFO)

	////////////***共通前準備***////////////

	//Instance interface
	var response RES.PostResponse

	/***Entityを登録する各処理へのキッカー***/
	//末尾に指定されたKind名がNsKindsで登録されていない場合、[NoDs]が付与されて以下のCaseで除外される
	switch 0 {

	/*=================================================================================
	Chained Tags Logging Load SAMPLE
	AttachmentのSearch APIで生産されたChained TagsのログをGCSから取得しBigQueryへロードする
	*/
	case
		strings.Index(rq.Urlpath, "/hmt/gcs/load/chained_tags_logging"),
		strings.Index(rq.Urlpath, "/hmt/gcs/load/chainedtags_logging"),
		strings.Index(rq.Urlpath, "/hmt/gcs/load/api_request_logging"):

		//GCSからデータをBqへロードする
		response.LoadGcs2BigQuery(w, r, &rq)

	/*=================================================================================
	画像チェック
	Attachmentの
	*/
	case
		strings.Index(rq.Urlpath, "/hmt/bq/load/checked_image_urls"):

		//GCSからデータをBqへロードする
		response.LoadBq2BigQuery(w, r, &rq)

		/*=================================================================================
		例外処理
		*/
	default:
		err := errors.New("【Error】path is nothing.")
		if err != nil {
			ERR.ErrorResponse(w, &rq, err, http.StatusBadRequest)
		}
	}
}
