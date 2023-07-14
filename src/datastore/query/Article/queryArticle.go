/*======================
Datastoreの
Namespace: Tester
kind: Article
に関するクエリ
========================*/
package query

import (
	"fmt"
	"net/http"
	"path/filepath"
	"runtime"
	"strings"

	DS_CONFIG "bwing.app/src/datastore/config"
	ENTITY "bwing.app/src/datastore/entity"
	QUERY "bwing.app/src/datastore/query"
	REQ "bwing.app/src/http/request"

	"github.com/pkg/errors"
	"google.golang.org/api/iterator"

	"cloud.google.com/go/datastore"
)

//ProjectId, Ns, Kind
var configArticle DS_CONFIG.DsConfig

///////////////////////////////////////////////////
//起動時にGCP ProjectID、NS, Kindを登録する
func init() {

	//Kind名を取得
	//pc, file, line, ok := runtime.Caller(0)
	_, file, _, _ := runtime.Caller(0)
	path := file
	f := filepath.Base(path[:len(path)-len(filepath.Ext(path))])
	k := strings.Replace(f, "query", "", 1)
	configArticle = ENTITY.SetDsConfig(k)
}

///////////////////////////////////////////////////
//Set Transaction
func NewTransaction(run bool) {
	//tran :=
}

//Interface
type QueryArticle struct{}

///////////////////////////////////////////////////
/* ===========================================
Aritcle kindをGetAllで取得する
※フィルターは、get parameterで指定
※Keyは返却される
* =========================================== */
func (q *QueryArticle) GetRunByFilterWithKey(rq *REQ.RequestData) (ENTITY.Entities, error) {

	//Article kindの箱を配列として準備
	var entity []ENTITY.EntityArticleWithKey

	//Articleの複数Entitiyを入れる箱を準備
	entities := ENTITY.NewEntities(entity)

	//クエリを作成
	var nq QUERY.GetDsQuery
	ctx, client, qry, err := nq.NewGetQueryIncludeClient(configArticle, rq)
	if err != nil {
		return entities, errors.WithStack(fmt.Errorf("%d, %w", http.StatusServiceUnavailable, err))
	}
	defer client.Close()

	//Article kindを取得
	t := client.Run(ctx, qry)
	for {
		var e ENTITY.EntityArticleWithKey
		key, err := t.Next(&e)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return entities, errors.WithStack(fmt.Errorf("%d, %w", http.StatusBadRequest, err))
		}
		e.Key.ID = key.ID
		e.Key.Name = key.Name
		entity = append(entity, e)
	}

	//配列に入れて返却
	entities.Entities = entity
	return entities, nil
}

///////////////////////////////////////////////////
/* ===========================================
//Article kindへ新規Entityを複数挿入
* =========================================== */
func (q QueryArticle) PutMultiUsingKeyArticle(ek []ENTITY.EntityKey, ens []ENTITY.EntityArticle) (int, error) {

	//Datastore clientを生成
	ctx, client, err := DS_CONFIG.NewClient()
	if err != nil {
		return 1, errors.WithStack(fmt.Errorf("%d, %w", http.StatusServiceUnavailable, err))
	}

	//Kindと名前空間を指定
	key := datastore.IncompleteKey(configArticle.Kind, nil)
	key.Namespace = configArticle.Ns

	//Datastore Keyをentityごとに設定（IDKey: Auto)
	var keys []*datastore.Key
	for i := range ens {
		if ek[i].ID != 0 {
			keys = append(keys, datastore.IDKey(configArticle.Kind, ek[i].ID, nil))
		} else if ek[i].Name != "" {
			keys = append(keys, datastore.NameKey(configArticle.Kind, ek[i].Name, nil))
		} else {
			keys = append(keys, datastore.IDKey(configArticle.Kind, 0, nil))
		}
		keys[i].Namespace = configArticle.Ns
	}

	//複数entityをDatastoreに挿入
	if _, err := client.PutMulti(ctx, keys, ens); err != nil {
		return 1, errors.WithStack(fmt.Errorf("%d, %w", http.StatusBadRequest, err))
	}

	//Success をリターン
	return 0, nil
}
