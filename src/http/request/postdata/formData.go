/*======================
POSTリクエストされたパラメーターの受信
FormDataを処理
========================*/
package postdata

import (
	"errors"
	"net/http"

	REQ "bwing.app/src/http/request"
)

///////////////////////////////////////////////////
func ParseFormData(w http.ResponseWriter, r *http.Request, rq *REQ.RequestData) error {

	var err error
	//Validation
	if rq.Method != "POST" {
		w.WriteHeader(http.StatusBadRequest)
		err = errors.New("Method")
		return err
	}
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		err = errors.New("Content-Type")
		return err
	}

	//Read form data
	for k, v := range r.Form {
		//fmt.Println(k, v)
		pp := REQ.PostParameter{Name: k, Type: "string", StringValue: v[0]}
		rq.PostParameter = append(rq.PostParameter, pp)
	}

	return nil
}
