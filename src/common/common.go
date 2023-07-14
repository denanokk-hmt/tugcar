/*======================
共通処理のファイル
========================*/
package common

import (
	"strconv"
	"strings"
	"time"
)

///////////////////////////////////////////////////
/* ===========================================
比較演算
ARGS
	ls: leftSideの値
	rs: rightSideの値
	ope: オペレーター
RETURN
	条件に一致:true/条件に不一致:false
=========================================== */
func Comparison(ls, rs, ope string) bool {

	var judge bool
	li, _ := strconv.Atoi(ls)
	ri, _ := strconv.Atoi(rs)

	//eq or ne以外は、Filter値を数値に変換した値で比較する
	switch ope {
	case "eq":
		if ls == rs {
			judge = true
		} else {
			judge = false
		}
	case "ne":
		if ls != rs {
			judge = true
		} else {
			judge = false
		}
	case "lt":
		if li < ri {
			judge = true
		} else {
			judge = false
		}
	case "gt":
		if li > ri {
			judge = true
		} else {
			judge = false
		}
	case "le":
		if li <= ri {
			judge = true
		} else {
			judge = false
		}
	case "ge":
		if li >= ri {
			judge = true
		} else {
			judge = false
		}
	}
	return judge
}

///////////////////////////////////////////////////
/* ===========================================
Chunkの開始と終了を計算
ARGS
	length: Chunkさせる配列の要素数
	chunkQty: chunkさせる数
	int: Chunk処理中のLoop index
	start: 配列から抽出する開始位置
	end:　配列から抽出する終了位置
	connter: Chunk数のカウンタ
RETURN
	Chunkする:true/Chunkしない:false
=========================================== */
func ChunkCalculator(length, chunkQty, idx int, start, end, counter *int) bool {

	var judge bool

	//Chunk数450でEntityを区切ってDSを更新していく(=DSの最大Update数500)
	if *counter == chunkQty {
		*counter = 0
	}
	if *counter == 0 {
		*start = idx
		*end = idx + chunkQty
		if *end > length {
			*end = length
		}
		judge = true
	}
	return judge
}

///////////////////////////////////////////////////
/* ===========================================
Chunkの開始と終了を計算
=========================================== */
type Chunks struct {
	Positions []Chunk
}
type Chunk struct {
	Start int
	End   int
	Qty   int
}

/* ===========================================
Chunkの開始と終了を計算
ARGS
	length: Chunkさせる配列の要素数
	chunkQty: chunkさせる数
RETURN
	Chunkする配列の抽出開始と終了位置
=========================================== */
func ChunkCalculator2(eLen, chunkQty int) Chunks {

	var counter, start, end int
	var chunks Chunks
	for idx := 0; idx < eLen; idx++ {
		if counter == chunkQty {
			counter = 0
		}
		if counter == 0 {
			start = idx
			end = idx + chunkQty
			if end > eLen {
				end = eLen
			}
			var chunk Chunk = Chunk{
				Start: start,
				End:   end,
				Qty:   end - start}
			chunks.Positions = append(chunks.Positions, chunk)
		}
		counter += 1
	}

	return chunks
}

///////////////////////////////////////////////////
/* ===========================================
日付差分
ARGS
	startDate: 開始日
	endDate: 終了日
	delimiter: 日付のデリミタ
RETURN
	日数
=========================================== */
func DateDiffCalculator(startDate, endDate, delimiter string) int {

	sArr := strings.Split(startDate, delimiter)
	sy, _ := strconv.Atoi(sArr[0])
	sm, _ := strconv.Atoi(sArr[1])
	sd, _ := strconv.Atoi(sArr[2])

	eArr := strings.Split(endDate, delimiter)
	ey, _ := strconv.Atoi(eArr[0])
	em, _ := strconv.Atoi(eArr[1])
	ed, _ := strconv.Atoi(eArr[2])

	start := time.Date(sy, time.Month(sm), sd, 0, 0, 0, 0, time.Local)
	end := time.Date(ey, time.Month(em), ed, 0, 0, 0, 0, time.Local)

	//Diff
	diffDays := end.Sub(start).Hours() / 24

	return int(diffDays)
}

///////////////////////////////////////////////////
/* ===========================================
日付追加
ARGS
	startDate: 開始日付
	delimiter: 日付のデリミタ
	addD: 追加日数
RETURN
	開始日付から日数分の日付を配列で戻す
=========================================== */
func DateAddCalculator(srcDate, delimiter string, addD int) []string {

	sArr := strings.Split(srcDate, delimiter)
	sy, _ := strconv.Atoi(sArr[0])
	sm, _ := strconv.Atoi(sArr[1])
	sd, _ := strconv.Atoi(sArr[2])

	var dArr []string
	for i := 0; i <= addD; i++ {
		t := time.Date(sy, time.Month(sm), sd, 0, 0, 0, 0, time.Local)
		t = t.AddDate(0, 0, i)
		ta := strings.Split(t.String(), " ")[0]
		ts := strings.Replace(ta, "-", "/", -1)
		dArr = append(dArr, ts)
	}

	return dArr
}

///////////////////////////////////////////////////
/* ===========================================
スライス割り
ARGS
	sliceLen: スライスの長さ
	devideQty: 割る数
RETURN
	開始位置、終了位置を配列で戻す
=========================================== */
func SliceDevideCalculator(sliceLen, devideQty int) (s, e []int) {
	//v := []int{1,2,3,4}
	//var s []int
	//var e []int

	l := sliceLen
	u := devideQty
	var buffE int
	for i := 0; i < u; i++ {

		if i == 0 {
			s = append(s, 0)
		} else {
			s = append(s, buffE)
		}
		if i == 0 {
			e = append(e, l/u)
		} else if u-1 == i {
			e = append(e, buffE+l-buffE)
		} else {
			e = append(e, l/u+buffE)
		}
		buffE = e[i]
	}

	/* testing
	for i, _ := range s {
		fmt.Println(v[s[i]:e[i]])
	}
	*/
	return s, e
}

///////////////////////////////////////////////////
/* ===========================================
重複を削除する
ARGS
	args: スライス
RETURN
	重複を除外したスライス
=========================================== */
func RemoveDuplicateArrayString(args []string) []string {
	results := make([]string, 0, len(args))
	encountered := map[string]bool{}
	for i := 0; i < len(args); i++ {
		if !encountered[args[i]] {
			encountered[args[i]] = true
			results = append(results, args[i])
		}
	}
	return results
}

///////////////////////////////////////////////////
/* ===========================================
スライス検索
ARGS
	slice: スライス
	value: 検索値
RETURN
	検索結果(あり:true/なし:false)
=========================================== */
func StringSliceSearch(slice []string, value string) bool {
	for _, s := range slice {
		if s == value {
			return true
		}
	}
	return false
}
