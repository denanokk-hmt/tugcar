/*======================
POSTリクエストされたパラメーターの受信
IamgeFileを処理
========================*/
package postdata

import (
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"

	"github.com/pkg/errors"
)

///////////////////////////////////////////////////
func ImageFile(w http.ResponseWriter, r *http.Request) (string, error) {

	const saveDir string = "./upload/image_file/"
	var err error

	// このハンドラ関数へのアクセスはPOSTメソッドのみ認める
	if r.Method != "POST" {
		err = errors.New("許可したメソッドとはことなります。")
		return "", err
	}
	var file multipart.File
	var fileHeader *multipart.FileHeader

	var uploadedFileName string
	// POSTされたファイルデータを取得する
	file, fileHeader, err = r.FormFile("image_file")
	fmt.Printf("%T", file)
	if err != nil {
		err = errors.New("ファイルアップロードを確認できませんでした。")
		return "", err
	}
	uploadedFileName = fileHeader.Filename
	var saveImage *os.File
	saveImage, err = os.Create(saveDir + uploadedFileName) //空ファイルを作成
	if err != nil {
		fmt.Fprintln(w, "")
		err = errors.New("ファイル確保できませんでした。")
		return "", err
	}
	defer saveImage.Close()
	defer file.Close()
	size, err := io.Copy(saveImage, file)
	if err != nil {
		err = errors.New("アップロードしたファイルの書き込みに失敗しました。")
		//os.Exit(1)
		return "", err
	}
	fmt.Println("書き込んだByte数=>")
	fmt.Println(size)

	return saveDir + uploadedFileName, nil
}
