/*
======================
BQからレコードを取得し、BigQueryへLoadする
========================
*/
package bigquery

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	TABLE "bwing.app/src/bigquery/table"
	COMMON "bwing.app/src/common"
	CONFIG "bwing.app/src/config"
	REQ "bwing.app/src/http/request"
	"google.golang.org/api/iterator"

	"cloud.google.com/go/bigquery"
)

// Inerface
type FetchBq struct{}

///////////////////////////////////////////////////
/* ===========================================
//Bqから取得した画像URLをチェックして、BigQueryへロードする
* =========================================== */
func (f FetchBq) FetchBqLoad2BqImageUrls(rq *REQ.RequestData) (int, error) {

	var err error

	/* ----------------------------------------
	Bqからレコードを取得
	---------------------------------------- */
	var qry string
	var dataset string
	var it *bigquery.RowIterator

	//Bqデータセット
	if rq.ParamsBasic.BqSource.Project != "" {
		//GCPのProjectを跨いでBigQueryデータを取得させたい場合
		//前提となるインフラの設定方法
		//https://svc.atlassian.net/wiki/spaces/CDLAB/pages/2302345274/Golang+Bigquery
		dataset = rq.ParamsBasic.BqSource.Project + "." + rq.ParamsBasic.BqSource.Dataset
	} else {
		//サーバーと同じGCPのプロジェクトのBigQueryデータを取得させる場合
		dataset = strings.ToLower(rq.ParamsBasic.ClientId)
	}

	//BiqQueryのTableIDを形成
	viewId := dataset + ".import_image_urls_check"

	//SQLの基本部分
	q_SELECT := `SELECT * `
	q_FROM := "FROM `" + viewId + "`"

	//追加SQL
	q_WHERE := ""
	q_LIMIT := ""
	qry = q_SELECT + q_FROM + q_WHERE + q_LIMIT

	//BigQueryからレコードを取得

	//BigQueryのclientを生成
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, CONFIG.GetConfig(CONFIG.PROJECT_ID))
	if err != nil {
		return 0, err
	}
	defer client.Close()

	//クエリを生成
	q := client.Query(qry)
	q.QueryConfig.UseStandardSQL = true

	//実行のためのqueryをサービスに送信してIteratorを通じて結果を返す
	it, err = q.Read(ctx)
	if err != nil {
		return 0, err
	}

	//BigQueryのレコードを格納する箱を準備(カラムごとに配列される)
	var valuess [][]bigquery.Value
	for {
		var values []bigquery.Value

		//これ以上結果が存在しない場合には、iterator.Doneを返して、ループ離脱
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
			fmt.Println(err)
			return 0, err
		}
		valuess = append(valuess, values)
	}

	//画像URLを取得
	var mainUrlsMap = make(map[string][]string)
	var checkSubUrlsMap = make(map[string][]string)
	for _, values := range valuess {
		bqUrls := values[4].([]bigquery.Value)
		var urls []string
		for i := 0; i < len(bqUrls); i++ {
			url := bqUrls[i].(string)
			urls = append(urls, url)
		}
		id := values[0].(string)
		var mUrl []string
		if values[3] != nil {
			url := values[3].(string)
			mUrl = append(mUrl, url)
			mainUrlsMap[id] = append(mainUrlsMap[id], mUrl...)
			checkSubUrlsMap[id] = append(checkSubUrlsMap[id], urls...)
		}
	}

	//画像チェック
	//goroutineで回した際に、ネットワーク負荷がかかりすぎる。もし可能な環境ならば、
	//コメントアウトしているgoroutineコードと入れ替えて実行することで、大幅な時間短縮が見込める(MutexでのLockを薦める)
	//1700件x10=17000件のチェック=約8分程度なので、許容とした。

	//var wg sync.WaitGroup
	//wg.Add(len(checkSubUrlsMap))

	cdt := time.Now()
	var subNewUrlsMap = make(map[string][]string)
	var sub10UrlMap = make(map[string]string) //sub10_LL.jpgの箱
	for id, urls := range checkSubUrlsMap {

		/*
			go func(wg *sync.WaitGroup, id string, urls []string) {
				defer wg.Done()*/
		func(id string, urls []string) {

			var newUrls []string
			for idx, url := range urls {
				resp, _ := http.Get(url)
				if resp != nil {
					defer resp.Body.Close()
					fmt.Println(id, url, resp.StatusCode)
					if resp.StatusCode == 200 {
						newUrls = append(newUrls, url)
						if idx == 9 {
							sub10UrlMap[id] = url //sub10_LL.jpgを確保しておく
						}
					}
				}
			}
			if len(newUrls) != 0 {
				subNewUrlsMap[id] = append(subNewUrlsMap[id], newUrls...)
			}

		}(id, urls)
		/*
			}(&wg, id, urls)
		*/
	}
	fmt.Printf("Finish!! 経過(seconds): %vns\n", time.Since(cdt).Seconds())
	//wg.Wait()

	//選別後のInsert用の箱にたたみ直し
	var iRecords []*TABLE.ImportImageUrls
	for id, _ := range subNewUrlsMap {
		var iRecord TABLE.ImportImageUrls
		iRecord.Cdt = cdt
		iRecord.ItemId = id
		if sub10UrlMap[id] != "" {
			iRecord.ImageMainUrls = append(iRecord.ImageMainUrls, sub10UrlMap[id])
			iRecord.ImageSubUrls = append(iRecord.ImageSubUrls, mainUrlsMap[id][0])
		} else {
			iRecord.ImageMainUrls = mainUrlsMap[id]
			iRecord.ImageSubUrls = subNewUrlsMap[id]
		}
		iRecords = append(iRecords, &iRecord)
	}

	/*------------------------------------------------
	BigQueryへログをインサート
	------------------------------------------------*/

	//BigQueryのclientを生成
	projectId := TABLE.GetProjectId()
	bqCtx := context.Background()
	bqClient, err := bigquery.NewClient(bqCtx, projectId)
	if err != nil {
		return 0, err
	}

	//BqにBulk Insertを行う
	rc, err := load2BqUrls(rq, iRecords, bqClient, &bqCtx)
	if err != nil {
		return 0, err
	}

	return rc, err
}

///////////////////////////////////////////////////
/* =========================================== */
//BigQueryに、UrlsをLoadする
/* =========================================== */
func load2BqUrls(rq *REQ.RequestData, records []*TABLE.ImportImageUrls, bqClient *bigquery.Client, bqCtx *context.Context) (int, error) {

	//for local testing
	if rq.ParamsBasic.BqNoInsert {
		fmt.Println("【【【 BqNoInsert is TRUE ß】】】")
		return 0, nil
	}

	//BiqQueryのTableID情報
	dataset := TABLE.DATASET_ATTACHMENT_FDC
	table := TABLE.TABLE_IMPORT_IMAGE_URLS

	//チャンクを計算
	var rss [][]*TABLE.ImportImageUrls
	chunks := COMMON.ChunkCalculator2(len(records), 10000)
	for _, c := range chunks.Positions {
		rs := records[c.Start:c.End]
		rss = append(rss, rs)
	}

	//Bqへバルクインサート
	for _, rs := range rss {
		u := bqClient.Dataset(dataset).Table(table).Uploader()
		err := u.Put(*bqCtx, rs)
		if err != nil {
			return 0, err
		}
	}

	return len(records), nil
}
