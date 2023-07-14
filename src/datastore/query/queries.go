/*======================
Datastoreのクエリ共通処理
========================*/
package query

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	CONFIG "bwing.app/src/config"
	DS_CONFIG "bwing.app/src/datastore/config"
	ENTITY "bwing.app/src/datastore/entity"
	REQ "bwing.app/src/http/request"

	"github.com/pkg/errors"

	"cloud.google.com/go/datastore"
)

///////////////////////////////////////////////////
//Set Transaction
func NewTransaction(run bool) {
	//tran :=
}

//Interface
type Queries struct {
	Tx *datastore.Transaction
}

///////////////////////////////////////////////////
/* ===========================================
最新Revisionを取得する
* =========================================== */
func (q *Queries) GetAllByRevision(rq *REQ.RequestData, rev, limit int, client *datastore.Client, dsKind string) (interface{}, []*datastore.Key, error) {

	//Filterを設定する箱を準備（連想配列名にフィルター名、要素に値）
	args := make(map[string]string)

	//Filterに必要な、Prop名&オペ、値(文字列)を設定
	if rev > 0 {
		args["filter_Revision_ieq"] = strconv.Itoa(rev)
	} else {
		args["filter_LatestRevision_beq"] = strconv.FormatBool(true)
	}
	args["limit"] = strconv.Itoa(limit)
	args["order_Revision"] = "desc"

	//Get parameterとしてFilterを設定(Get parameterを初期化)
	REQ.SettingFilterToGetParamter(rq, &args, true)

	//Entityを取得
	entity, pKeys, err := q.GetAllByFilter(rq, client, dsKind, nil)
	if err != nil {
		return nil, nil, err
	}

	return entity, pKeys, nil
}

///////////////////////////////////////////////////
/* ===========================================
kindをGetAllで取得する
※フィルターは、rq.GetParameterで指定
* =========================================== */
func (q *Queries) GetAllByFilterIncludeDsCient(rq *REQ.RequestData) (ENTITY.Entities, []ENTITY.EntityKey, []*datastore.Key, error) {

	//DatastoreのConfigとEntitiesをセット
	cg, ena := ENTITY.NewConfigEntities(rq.ParamsBasic.Kind)

	cg.Ns = cg.Ns + "-" + rq.ParamsBasic.ClientId + "-" + CONFIG.GetConfig("Env")

	//mapをキャスト
	et := ena.(map[string]ENTITY.Entities)

	//kindの箱を配列として準備
	ens := et[cg.Kind].Entities

	//共有するためのEntitiesを更にを入れる箱を準備
	enss := ENTITY.NewEntities(ens)

	//クエリを作成
	var nq GetDsQuery
	ctx, client, qry, err := nq.NewGetQueryIncludeClient(cg, rq)
	if err != nil {
		return enss, nil, nil, errors.WithStack(fmt.Errorf("%d, %w", http.StatusServiceUnavailable, err))
	}

	//kindを取得
	keys, err := client.GetAll(ctx, qry, ens)
	if err != nil {
		return enss, nil, nil, errors.WithStack(fmt.Errorf("%d, %w", http.StatusBadRequest, err))
	}

	//Key情報を取得
	var eks []ENTITY.EntityKey
	for _, k := range keys {
		var ek ENTITY.EntityKey = ENTITY.EntityKey{
			ID: k.ID,
		}
		eks = append(eks, ek)
	}

	//配列に入れて返却
	enss.Entities = ens
	return enss, eks, keys, nil
}

///////////////////////////////////////////////////
/* ===========================================
kindをGetAllで取得する
datastore.Clientを共通利用→引数
※フィルターは、rq.GetParameterで指定
* =========================================== */
func (q *Queries) GetAllByFilter(rq *REQ.RequestData, client *datastore.Client, dsKind string, pKey *datastore.Key) (interface{}, []*datastore.Key, error) {

	//DatastoreのConfigとEntitiesをセット
	cg, ena := ENTITY.NewConfigEntities(dsKind)

	//Namsespcaを設定
	cg.Ns = cg.Ns + "-" + rq.ParamsBasic.ClientId + "-" + CONFIG.GetConfig("Env")

	//mapをキャスト
	et := ena.(map[string]ENTITY.Entities)

	//kindの箱を配列として準備
	ens := et[dsKind].Entities

	//クエリを作成
	var nq GetDsQuery
	ctx, qry, err := nq.NewGetQuery(cg, rq, pKey, false)
	if err != nil {
		return nil, nil, errors.WithStack(fmt.Errorf("%d, %w", http.StatusServiceUnavailable, err))
	}

	//kindを取得
	keys, err := client.GetAll(ctx, qry, ens)
	if err != nil {
		return nil, nil, errors.WithStack(fmt.Errorf("%d, %w", http.StatusBadRequest, err))
	}

	return ens, keys, nil
}

///////////////////////////////////////////////////
/* ===========================================
kindをGetAllで取得する
datastore.Clientを共通利用→引数
※フィルターは、rq.GetParameterSyncMapで指定
* =========================================== */
func (q *Queries) GetAllByFilterSyncMap(rq *REQ.RequestData, client *datastore.Client, dsKind string, pKey *datastore.Key, mapKey string) (interface{}, []*datastore.Key, error) {

	//DatastoreのConfigとEntitiesをセット
	cg, ena := ENTITY.NewConfigEntities(dsKind)

	//Namsespcaを設定
	cg.Ns = cg.Ns + "-" + rq.ParamsBasic.ClientId + "-" + CONFIG.GetConfig("Env")

	//mapをキャスト
	et := ena.(map[string]ENTITY.Entities)

	//kindの箱を配列として準備
	ens := et[dsKind].Entities

	//クエリを作成
	var nq GetDsQuery
	ctx, qry, err := nq.NewGetQuerySyncMap(cg, rq, pKey, false, mapKey)
	if err != nil {
		return nil, nil, errors.WithStack(fmt.Errorf("%d, %w", http.StatusServiceUnavailable, err))
	}

	//kindを取得
	keys, err := client.GetAll(ctx, qry, ens)
	if err != nil {
		return nil, nil, errors.WithStack(fmt.Errorf("%d, %w", http.StatusBadRequest, err))
	}

	return ens, keys, nil
}

///////////////////////////////////////////////////
/* ===========================================
kindを__Key__を使って取得する
※Keyのフィルターは、get parameterで指定
=========================================== */
func (q *Queries) GetByKey(rq *REQ.RequestData, client *datastore.Client, dsKind string, pKey *datastore.Key) (interface{}, error) {

	//DatastoreのConfigとEntitiesをセット
	cg, ena := ENTITY.NewConfigEntities(dsKind)

	//Namsespcaを設定
	cg.Ns = cg.Ns + "-" + rq.ParamsBasic.ClientId + "-" + CONFIG.GetConfig("Env")

	//mapをキャスト
	et := ena.(map[string]ENTITY.Entities)

	//kindの箱を配列として準備
	entity := et[cg.Kind].Entities

	//複数Entitiyを入れる箱を準備
	e := ENTITY.NewEntities(entity)

	//Datastore clientを生成
	ctx := context.Background()

	//Get parameterをインターフェースにセット
	var ig interface{}
	if len(rq.GetParameter) != 0 {
		ig = interface{}(&rq.GetParameter)
	}

	//クエリオプションを登録
	op := NewGetOptions(ig)

	//Kind, Keyと名前空間を指定
	var key *datastore.Key
	if op.Key.Id != 0 {
		key = DS_CONFIG.NewIDKey(cg.Kind, op.Key.Id, pKey)
	} else if op.Key.Name != "" {
		key = DS_CONFIG.NewNameKey(cg.Kind, op.Key.Name, pKey)
	} else {
		err := errors.New("key is nothing.")
		return e, errors.WithStack(fmt.Errorf("%d, %w", http.StatusBadRequest, err))
	}
	key.Namespace = cg.Ns

	//entityを取得
	err := client.Get(ctx, key, entity)
	if err != nil {
		return e, errors.WithStack(fmt.Errorf("%d, %w", http.StatusBadRequest, err))
	}

	return e.Entities, nil
}

///////////////////////////////////////////////////
/* ===========================================
kindを__Key__を使って取得する
※フィルターは、rq.GetParameterSyncMapで指定
=========================================== */
func (q *Queries) GetByKeySyncMap(rq *REQ.RequestData, client *datastore.Client, dsKind string, pKey *datastore.Key, mapKey string) (interface{}, error) {

	//DatastoreのConfigとEntitiesをセット
	cg, ena := ENTITY.NewConfigEntities(dsKind)

	//Namsespcaを設定
	cg.Ns = cg.Ns + "-" + rq.ParamsBasic.ClientId + "-" + CONFIG.GetConfig("Env")

	//mapをキャスト
	et := ena.(map[string]ENTITY.Entities)

	//kindの箱を配列として準備(※注意：OptionalのKind名を設定してある場合、cg.Kindではなく、argのdsKind利用が正解)
	entity := et[dsKind].Entities

	//複数Entitiyを入れる箱を準備
	e := ENTITY.NewEntities(entity)

	//Datastore clientを生成
	ctx := context.Background()

	//Get parameterをインターフェースにセット
	var ig interface{}
	m, ok := rq.GetParameterSyncMap.Load(mapKey)
	if ok {
		ig = interface{}(m)
	}

	//クエリオプションを登録
	op := *NewGetOptionsMap(ig)

	//Kind, Keyと名前空間を指定
	var key *datastore.Key
	if op.Key.Id != 0 {
		key = DS_CONFIG.NewIDKey(cg.Kind, op.Key.Id, pKey)
	} else if op.Key.Name != "" {
		key = DS_CONFIG.NewNameKey(cg.Kind, op.Key.Name, pKey)
	} else {
		err := errors.New("key is nothing.")
		return e, errors.WithStack(fmt.Errorf("%d, %w", http.StatusBadRequest, err))
	}
	key.Namespace = cg.Ns

	//entityを取得
	err := client.Get(ctx, key, entity)
	if err != nil {
		return e, errors.WithStack(fmt.Errorf("%d, %w", http.StatusBadRequest, err))
	}

	return e.Entities, nil
}

///////////////////////////////////////////////////
/* ===========================================
//Aritcle kindをKeyを使って取得する
//Transaction
* =========================================== */
func (q *Queries) GetByKeyTran(rq *REQ.RequestData, dsKind string) (ENTITY.Entities, error) {

	//DatastoreのConfigとEntitiesをセット
	cg, ena := ENTITY.NewConfigEntities(dsKind)

	//mapをキャスト
	et := ena.(map[string]ENTITY.Entities)

	//kindの箱を配列として準備
	entity := et[cg.Kind].Entities

	//複数Entitiyを入れる箱を準備
	e := ENTITY.NewEntities(entity)

	//Datastore clientを生成
	ctx, client, err := DS_CONFIG.NewClient()
	if err != nil {
		return e, errors.WithStack(fmt.Errorf("%d, %w", http.StatusServiceUnavailable, err))
	}

	//Get parameterをインターフェースにセット
	var ig interface{}
	if len(rq.GetParameter) != 0 {
		ig = interface{}(&rq.GetParameter)
	}

	//クエリオプションを登録
	op := NewGetOptions(ig)

	//Kind, Keyと名前空間を指定
	var key *datastore.Key
	if op.Key.Id != 0 {
		key = DS_CONFIG.NewIDKey(cg.Kind, op.Key.Id, nil)
	} else if op.Key.Name != "" {
		key = DS_CONFIG.NewNameKey(cg.Kind, op.Key.Name, nil)
	} else {
		err := errors.New("key is nothing.")
		return e, errors.WithStack(fmt.Errorf("%d, %w", http.StatusBadRequest, err))
	}
	key.Namespace = cg.Ns

	const retries = 3
	var tx *datastore.Transaction
	for i := 0; i < retries; i++ {
		tx, err = client.NewTransaction(ctx)
		if err != nil {
			break
		}
		//entityを取得
		if err = tx.Get(key, entity); err != nil && err != datastore.ErrNoSuchEntity {
			break
		}
		//Article kindのKeyを設定
		//e.Key.ID = op.Key.Id

		//Commit
		if _, err = tx.Commit(); err != datastore.ErrConcurrentTransaction {
			break
		}
	}
	//Error Handle
	if err != nil {
		return e, errors.WithStack(fmt.Errorf("%d, %w", http.StatusBadRequest, err))
	}

	return e, nil
}

///////////////////////////////////////////////////
/* ===========================================
//Keyを使って、PutMultiでEntityを更新する
* =========================================== */
func (q *Queries) PutMultiUsingKey(rq *REQ.RequestData, pKeys, eKeys []*datastore.Key, ens interface{}, lenEns int,
	client *datastore.Client, dsKind string) (int, error) {

	//Datastore Namespaceを設定
	//DatastoreのConfigとEntitiesをセット
	cg, _ := ENTITY.NewConfigEntities(dsKind)
	cg.Ns = cg.Ns + "-" + rq.ParamsBasic.ClientId + "-" + CONFIG.GetConfig("Env")

	//contextを生成
	ctx := context.Background()

	//Datastore Keyをentityごとに設定
	var keys []*datastore.Key
	for i := 0; i < lenEns; i++ {

		//EntityごとのKeyの初期値を準備
		var pKey *datastore.Key = nil
		var eKeyID int64 = 0
		var eKeyName string = ""

		//親キーあれば指定、なければnil
		if pKeys != nil {
			if eKeys != nil && pKeys[i].Kind == eKeys[i].Kind {
				pKey = pKeys[i].Parent //自分自身を上書き更新するが、親キーを持っている場合
			} else {
				pKey = pKeys[i]
			}
		}

		//IDKey or NameKeyを指定、なければIDKey=0:Auto
		if eKeys != nil {
			if eKeys[i].ID != 0 {
				eKeyID = eKeys[i].ID
			} else {
				eKeyName = eKeys[i].Name
			}
		}

		//DatastoreのKeyを登録
		switch {
		case eKeyID > 0: //親キー:ありorなし、IDKey:あり
			keys = append(keys, datastore.IDKey(cg.Kind, eKeys[i].ID, pKey))
		case eKeyName != "": //親キー:ありorなし、NameKey:あり
			keys = append(keys, datastore.NameKey(cg.Kind, eKeys[i].Name, pKey))
		default: //親キー:ありorなし、IDKey:Auto
			keys = append(keys, datastore.IDKey(cg.Kind, 0, pKey))
		}

		//名前空間を指定
		keys[i].Namespace = cg.Ns
	}

	//複数entityをDatastoreに挿入
	if _, err := client.PutMulti(ctx, keys, ens); err != nil {
		return 1, errors.WithStack(fmt.Errorf("%d, %w", http.StatusBadRequest, err))
	}

	//Success をリターン
	return 0, nil
}

///////////////////////////////////////////////////
/* ===========================================
//Keyを使って、PutMultiでEntityを更新する
* =========================================== */
func (q *Queries) PutMultiUsingKeyTran(rq *REQ.RequestData, pKeys, eKeys []*datastore.Key, ens interface{}, lenEns int,
	tx *datastore.Transaction, dsKind string) (int, error) {

	//Datastore Namespaceを設定
	//DatastoreのConfigとEntitiesをセット
	cg, _ := ENTITY.NewConfigEntities(dsKind)
	cg.Ns = cg.Ns + "-" + rq.ParamsBasic.ClientId + "-" + CONFIG.GetConfig("Env")

	//contextを生成
	//ctx := context.Background()

	//Datastore Keyをentityごとに設定
	var keys []*datastore.Key
	for i := 0; i < lenEns; i++ {

		//EntityごとのKeyの初期値を準備
		var pKey *datastore.Key = nil
		var eKeyID int64 = 0
		var eKeyName string = ""

		//親キーあれば指定、なければnil
		if pKeys != nil {
			if eKeys != nil && pKeys[i].Kind == eKeys[i].Kind {
				pKey = pKeys[i].Parent //自分自身を上書き更新するが、親キーを持っている場合
			} else {
				pKey = pKeys[i]
			}
		}

		//IDKey or NameKeyを指定、なければIDKey=0:Auto
		if eKeys != nil {
			if eKeys[i].ID != 0 {
				eKeyID = eKeys[i].ID
			} else {
				eKeyName = eKeys[i].Name
			}
		}

		//DatastoreのKeyを登録
		switch {
		case eKeyID > 0: //親キー:ありorなし、IDKey:あり
			keys = append(keys, datastore.IDKey(cg.Kind, eKeys[i].ID, pKey))
		case eKeyName != "": //親キー:ありorなし、NameKey:あり
			keys = append(keys, datastore.NameKey(cg.Kind, eKeys[i].Name, pKey))
		default: //親キー:ありorなし、IDKey:Auto
			keys = append(keys, datastore.IDKey(cg.Kind, 0, pKey))
		}

		//名前空間を指定
		keys[i].Namespace = cg.Ns
	}

	//複数entityをDatastoreに挿入
	//if _, err := client.PutMulti(ctx, keys, ens); err != nil {
	if _, err := tx.PutMulti(keys, ens); err != nil {
		return 1, errors.WithStack(fmt.Errorf("%d, %w", http.StatusBadRequest, err))
	}

	//Success をリターン
	return 0, nil
}

///////////////////////////////////////////////////
/* ===========================================
//Keyを使って、PutMultiでEntityを更新する
* =========================================== */
func (q *Queries) PutMultiUsingWithChannel(rq *REQ.RequestData,
	nks []string, pkMaps map[string]*datastore.Key, eKeys []*datastore.Key,
	ens interface{}, lenEns int,
	client *datastore.Client, dsKind string,
	ch chan<- bool) (int, error) {

	defer close(ch)

	//Datastore Namespaceを設定
	//DatastoreのConfigとEntitiesをセット
	cg, _ := ENTITY.NewConfigEntities(dsKind)
	cg.Ns = cg.Ns + "-" + rq.ParamsBasic.ClientId + "-" + CONFIG.GetConfig("Env")

	//contextを生成
	ctx := context.Background()

	//Datastore Keyをentityごとに設定
	var keys []*datastore.Key
	for i := 0; i < lenEns; i++ {

		//EntityごとのKeyの初期値を準備
		var pKey *datastore.Key = nil
		var eKeyID int64 = 0
		var eKeyName string = ""

		pKeys := pkMaps[nks[i]]

		//親キーあれば指定、なければnil
		if pKeys != nil {
			if eKeys != nil && pKeys.Kind == eKeys[i].Kind {
				pKey = pKeys.Parent //自分自身を上書き更新するが、親キーを持っている場合
			} else {
				pKey = pKeys
			}
		}

		//IDKey or NameKeyを指定、なければIDKey=0:Auto
		if eKeys != nil {
			if eKeys[i].ID != 0 {
				eKeyID = eKeys[i].ID
			} else {
				eKeyName = eKeys[i].Name
			}
		}

		//DatastoreのKeyを登録
		switch {
		case eKeyID > 0: //親キー:ありorなし、IDKey:あり
			keys = append(keys, datastore.IDKey(cg.Kind, eKeys[i].ID, pKey))
		case eKeyName != "": //親キー:ありorなし、NameKey:あり
			keys = append(keys, datastore.NameKey(cg.Kind, eKeys[i].Name, pKey))
		default: //親キー:ありorなし、IDKey:Auto
			keys = append(keys, datastore.IDKey(cg.Kind, 0, pKey))
		}

		//名前空間を指定
		keys[i].Namespace = cg.Ns
	}

	//複数entityをDatastoreに挿入
	if _, err := client.PutMulti(ctx, keys, ens); err != nil {
		return 1, errors.WithStack(fmt.Errorf("%d, %w", http.StatusBadRequest, err))
	}

	ch <- true

	//Success をリターン
	return 0, nil
}

///////////////////////////////////////////////////
/* ===========================================
//kindへ新規Entityを1件挿入
* =========================================== */
func (q *Queries) PutUsingKey(ek ENTITY.EntityKey, en *ENTITY.Entities, k, m string) (int, error) {

	//DatastoreのConfigのみをセット
	cg, _ := ENTITY.NewConfigEntities(k)

	//kindの箱を配列として準備
	e := en.Entities

	//Datastore clientを生成
	ctx, client, err := DS_CONFIG.NewClient()
	if err != nil {
		return 1, errors.WithStack(fmt.Errorf("%d, %w", http.StatusServiceUnavailable, err))
	}
	defer client.Close()

	//Kindを指定
	var key *datastore.Key

	//Keyを指定
	if ek.ID != 0 {
		key = datastore.IDKey(cg.Kind, ek.ID, nil)
	} else if ek.Name != "" {
		key = datastore.NameKey(cg.Kind, ek.Name, nil)
	} else {
		key = datastore.IDKey(cg.Kind, 0, nil)
	}

	//名前空間を指定
	key.Namespace = cg.Ns
	//entityを確認
	if m == "PUT" {
		if err = client.Get(ctx, key, e); err != nil {
			return 1, errors.WithStack(fmt.Errorf("%d, %w", http.StatusBadRequest, err))
		}
	}
	//新規にEntityを追加
	if _, err := client.Put(ctx, key, e); err != nil {
		return 1, errors.WithStack(fmt.Errorf("%d, %w", http.StatusBadRequest, err))
	}

	return 0, nil
}

///////////////////////////////////////////////////
/* ===========================================
//Article kindへ新規Entityを1件挿入
//Transaction
* =========================================== */
func (q *Queries) PutUsingKeyTran(ek ENTITY.EntityKey, en *ENTITY.Entities, k, m string) (int, error) {

	//DatastoreのConfigのみをセット
	cg, _ := ENTITY.NewConfigEntities(k)

	//kindの箱を配列として準備
	e := en.Entities

	//Datastore clientを生成
	ctx, client, err := DS_CONFIG.NewClient()
	if err != nil {
		return 1, errors.WithStack(fmt.Errorf("%d, %w", http.StatusServiceUnavailable, err))
	}
	defer client.Close()

	//Kindを指定
	var key *datastore.Key

	//Keyを指定
	if ek.ID != 0 {
		key = datastore.IDKey(cg.Kind, ek.ID, nil)
	} else if ek.Name != "" {
		key = datastore.NameKey(cg.Kind, ek.Name, nil)
	} else {
		key = datastore.IDKey(cg.Kind, 0, nil)
	}

	//名前空間を指定
	key.Namespace = cg.Ns

	const retries = 3
	var tx *datastore.Transaction
	for i := 0; i < retries; i++ {
		tx, err = client.NewTransaction(ctx)
		if err != nil {
			return 1, errors.WithStack(fmt.Errorf("%d, %w", http.StatusUnprocessableEntity, err))
		}
		//Article entityを取得
		if m == "PUT" {
			if err = tx.Get(key, e); err != nil {
				break
			}
		}
		//新規にEntityを追加
		if _, err = tx.Put(key, e); err != nil {
			break
		}
		//Commit
		if _, err = tx.Commit(); err != datastore.ErrConcurrentTransaction {
			break
		}
	}
	//Error Handle
	if err != nil {
		return 1, errors.WithStack(fmt.Errorf("%d, %w", http.StatusBadRequest, err))
	}

	return 0, nil
}

///////////////////////////////////////////////////
/* ===========================================
//kindをKeyを使って削除する
//Transactionなし
* =========================================== */
func (q *Queries) DeleteByKey(ek ENTITY.EntityKey, dsKind string) (int, error) {

	//DatastoreのConfigのみをセット
	cg, _ := ENTITY.NewConfigEntities(dsKind)

	//Datastore clientを生成
	ctx, client, err := DS_CONFIG.NewClient()
	if err != nil {
		return 1, errors.WithStack(fmt.Errorf("%d, %w", http.StatusServiceUnavailable, err))
	}
	defer client.Close()

	//Kindを指定
	var key *datastore.Key

	//Keyを指定
	if ek.ID != 0 {
		key = datastore.IDKey(cg.Kind, ek.ID, nil)
	} else if ek.Name != "" {
		key = datastore.NameKey(cg.Kind, ek.Name, nil)
	} else {
		err := errors.New("No_DS_Key")
		return 1, errors.WithStack(fmt.Errorf("%d, %w", http.StatusServiceUnavailable, err))
	}

	//名前空間を指定
	key.Namespace = cg.Ns

	//Entityを削除
	if err := client.Delete(ctx, key); err != nil {
		return 1, errors.WithStack(fmt.Errorf("%d, %w", http.StatusBadRequest, err))
	}

	return 0, nil
}

///////////////////////////////////////////////////
/* ===========================================
//kindをKeyを使って削除する
//Transactionあり
* =========================================== */
func (q *Queries) DeleteByKeyTran(ek ENTITY.EntityKey, dsKind string) (int, error) {

	//DatastoreのConfigのみをセット
	cg, ena := ENTITY.NewConfigEntities(dsKind)

	//mapをキャスト
	et := ena.(map[string]ENTITY.Entities)

	//kindの箱を配列として準備
	e := et[cg.Kind].Entities

	//Datastore clientを生成
	ctx, client, err := DS_CONFIG.NewClient()
	if err != nil {
		return 1, errors.WithStack(fmt.Errorf("%d, %w", http.StatusServiceUnavailable, err))
	}
	defer client.Close()

	//Kindを指定
	var key *datastore.Key

	//Keyを指定
	if ek.ID != 0 {
		key = datastore.IDKey(cg.Kind, ek.ID, nil)
	} else if ek.Name != "" {
		key = datastore.NameKey(cg.Kind, ek.Name, nil)
	} else {
		err := errors.New("No_DS_Key")
		return 1, errors.WithStack(fmt.Errorf("%d, %w", http.StatusServiceUnavailable, err))
	}

	//名前空間を指定
	key.Namespace = cg.Ns

	const retries = 3
	var tx *datastore.Transaction
	for i := 0; i < retries; i++ {
		tx, err = client.NewTransaction(ctx)
		if err != nil {
			break
		}
		//entityを取得
		if err = tx.Get(key, &e); err != nil {
			break
		}
		//Entityを削除
		if err := tx.Delete(key); err != nil {
			return 1, errors.WithStack(fmt.Errorf("%d, %w", http.StatusBadRequest, err))
		}
		//Commit
		if _, err = tx.Commit(); err != datastore.ErrConcurrentTransaction {
			break
		}
	}
	//Error Handle
	if err != nil {
		return 1, errors.WithStack(fmt.Errorf("%d, %w", http.StatusBadRequest, err))
	}

	return 0, nil
}

///////////////////////////////////////////////////
/* ===========================================
//AttachmentのkindをKeyを使って削除する
//Transactionなし
* =========================================== */
func (q *Queries) DeleteLoadDataByKey(rq *REQ.RequestData, ek ENTITY.EntityKey, pKey *datastore.Key, client *datastore.Client) (int, error) {

	//DatastoreのConfigのみをセット
	cg, _ := ENTITY.NewConfigEntities(rq.ParamsBasic.Kind)
	cg.Ns = cg.Ns + "-" + rq.ParamsBasic.ClientId + "-" + CONFIG.GetConfig("Env")

	//Kindを指定
	var key *datastore.Key

	//Keyを指定
	if pKey != nil {
		key = datastore.IDKey(cg.Kind, ek.ID, pKey)
	} else if ek.ID != 0 {
		key = datastore.IDKey(cg.Kind, ek.ID, nil)
	} else if ek.Name != "" {
		key = datastore.NameKey(cg.Kind, ek.Name, nil)
	} else {
		err := errors.New("No_DS_Key")
		return 1, errors.WithStack(fmt.Errorf("%d, %w", http.StatusServiceUnavailable, err))
	}

	//名前空間を指定
	key.Namespace = cg.Ns

	//Entityを削除
	if err := client.Delete(context.Background(), key); err != nil {
		return 1, errors.WithStack(fmt.Errorf("%d, %w", http.StatusBadRequest, err))
	}

	return 0, nil
}
