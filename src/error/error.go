/*======================
エラーハンドル
========================*/
package error

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	REQ "bwing.app/src/http/request"
	LOG "bwing.app/src/log"
	"github.com/pkg/errors"
)

type OsSettingsStruct struct {
	Hostname string
}

var OsSettings OsSettingsStruct

type ErrorLogs struct {
	Error   error
	Host    string
	Method  string
	Urlpath string
	Headers http.Header
	Params  string
}

func init() {
	n, _ := os.Hostname()
	OsSettings.Hostname = n
	//fmt.Println(OsSettings)
}

//Error responseの箱
type ErrorResult struct {
	HttpStatus int
	Error      []string
}

///////////////////////////////////////////////////
/* ===========================================
//Errorの結果のロギング
err は、WithStackで包んで渡してくることを必須とする
例: errors.WithStack(fmt.Errorf("%d, %w", http.StatusBadRequest, err))
* =========================================== */
func ErrorLoggingWithStackTrace(rq *REQ.RequestData, err error) *ErrorResult {

	//出力項目
	var output ErrorLogs = ErrorLogs{
		Error:   err,
		Host:    OsSettings.Hostname,
		Method:  rq.Method,
		Urlpath: rq.Urlpath,
		Headers: rq.Header.(http.Header),
		Params:  rq.ParamsBasic.ParamsStrings,
	}

	//Error logging with stacktrace
	fmt.Println(LOG.SetLogEntry(LOG.ERROR, "Errorlogging", fmt.Sprintf("%+v\n", output)))

	//Set http status, error msg for error response
	var er ErrorResult
	msg := strings.Split(err.Error(), ",")
	er.HttpStatus, _ = strconv.Atoi(msg[0])
	s := make([]string, 0)
	if len(msg) < 2 {
		s = append(s, "error")
	} else {
		s = append(s, msg[1])
	}
	er.Error = s
	return &er
}

///////////////////////////////////////////////////
/* ===========================================
////Errorの結果をレスポンス
* =========================================== */
func ErrorResponse(w http.ResponseWriter, rq *REQ.RequestData, err error, errStatus int) {

	//Status
	if errStatus == 0 {
		errStatus = http.StatusInternalServerError
	}

	//Servity:WARN Request logging
	LOG.ApiRequestLogging(rq, LOG.WARN)

	//Severity:ERROR Error logging
	//fmt.Println(LOG.SetLogEntry(LOG.ERROR, "Errorlogging", fmt.Sprintf("%+v\n", err)))

	//Error logging with stacktrace
	er := ErrorLoggingWithStackTrace(rq, errors.WithStack(fmt.Errorf("%d, %w", errStatus, err)))

	//Response
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusBadRequest)
	json.NewEncoder(w).Encode(er)
}
