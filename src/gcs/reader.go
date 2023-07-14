/*
======================
BigQueryからItemマスターデータを取得する処理
========================
*/
package bigquery

import (
	"bytes"
	"context"
	"time"

	//共通Query
	//共通Query
	//Items専用Query

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

///////////////////////////////////////////////////
/* =========================================== */
// Chained Tags loggingをBigQueryへLoadする
/* =========================================== */
func ObjectPathGetter(client *storage.Client, bucket string, prefix string) ([]string, context.Context, error) {

	var err error

	//Contextを
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Minute*10)
	defer cancel()

	//GCS clientを生成
	if client == nil {
		client, err = storage.NewClient(ctx)
		if err != nil {
			return nil, ctx, err
		}
		defer client.Close()
	}

	//Queryを指定
	query := &storage.Query{
		Prefix: prefix,
		//Prefix: "run.googleapis.com/stdout/2022/07",
		//StartOffset: "2022/07", // Only list objects lexicographically >= "bar/"
		//EndOffset:   "07/",     // Only list objects lexicographically < "foo/"
	}

	//指定したフォルダにあるオブジェクトパス一覧の取得
	var paths []string
	it := client.Bucket(bucket).Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, ctx, err
		}
		paths = append(paths, attrs.Name)
	}

	return paths, ctx, nil
}

///////////////////////////////////////////////////
/* =========================================== */
// Chained Tags loggingをBigQueryへLoadする
/* =========================================== */
func ObjectReader(client *storage.Client, bucket string, paths []string) ([]string, context.Context, error) {

	var err error

	//GCS clientを生成
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Minute*10)
	defer cancel()
	if client == nil {
		client, err = storage.NewClient(ctx)
		if err != nil {
			return nil, ctx, err
		}
		defer client.Close()
	}

	//Path指定されたオブジェクトを取得(取得出来なかったものは、空文字)
	var logs []string
	for _, p := range paths {
		var log string

		//Bucket Readerで読み込み
		reader, err := client.Bucket(bucket).Object(p).NewReader(ctx)
		if err != nil {
			return nil, ctx, err
		}
		defer reader.Close()

		//バッファリング
		var buf bytes.Buffer
		if _, err := buf.ReadFrom(reader); err != nil {
			return nil, ctx, err
		}

		//文字列に変換して格納（軽さより扱いやすさで）
		log = string(buf.Bytes())
		logs = append(logs, log)
	}

	return logs, ctx, nil
}
