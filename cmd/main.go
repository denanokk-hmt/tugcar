/* =================================
サーバー起動時の起点ファイル
HttpサーバーのListen、各種初期設定を行うスタンバイ
* ================================= */
package main

import (
	"fmt"
	"strings"

	CONFIG "bwing.app/src/config"
	HTTP "bwing.app/src/http"
)

func main() {

	//Server間認証用のTokenを格納
	CONFIG.NewUuv4Tokens()

	//Request routing & Server listen
	HTTP.HandleRequests()
}

// N-gramをstringの配列型で返す
func callNgramTest() {
	//Ngram sample
	target_string := "徒然なるままに"
	bigrams, bool := ngram(target_string, 2)
	if !bool {
		fmt.Println("エラー")
	}
	fmt.Println("Bigrams: ", bigrams)
}
func ngram(target_text string, n int) ([]string, bool) {
	sep_text := strings.Split(target_text, "")
	var ngrams []string
	if len(sep_text) < n {
		fmt.Println("Error: Input string's length is less than n value")
		return nil, false
	}
	for i := 0; i < (len(sep_text) - n + 1); i++ {
		ngrams = append(ngrams, strings.Join(sep_text[i:i+n], ""))
	}
	return ngrams, true
}
