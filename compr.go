package main

import (
    "bytes"
    "io"
    "os"
    "math"
    "fmt"
    "compress/gzip"
    "math/rand"
    "time"
)

const pageSize = 16384
const hintSize = 64
const numHintsToTry = 3
const entrThreshold = 5.0

var logLut [256]float64

//
// Initialize the log lookup table. Calculate the log base 2 values
// using the change of log base formula
//
func initLogLut() {
    l2Bval := math.Log(2.0)

    numSegs := pageSize / hintSize

    for ind := 1; ind < numSegs; ind++ {
        logLut[ind] = (math.Log(float64(ind)) / l2Bval)
    }
}

//
// If there is an error, print the msg string and then panic.
//
func checkError(err error, msg string) {
    if err != nil {
        fmt.Printf("%s\n", msg)
        panic(err)
    }
}

//
// Initialize the log lookup table
//

//
// Init and return a segment number hint array.
//
func initHintArr() []int {
    hLen := pageSize / hintSize

    hintArr := make([]int, hLen)

    for i := 0; i < hLen; i++ {
        hintArr[i] = i
    }

    return hintArr
}

//
// Get a random segment number from the list of segment numbers in
// hintArr. A new slice with the returned segment number removed is
// returned.
//
func getRandomSeg(hintArr []int) (int, []int) {
    arrLen := len(hintArr)

    segInd := rand.Intn(arrLen)
    segNum := hintArr[segInd]

    hintArr[segInd] = hintArr[arrLen - 1]

    retArr := hintArr[:(arrLen - 1)]

    return segNum, retArr
}

func genHist(data []byte, segNum int) ([]int, int) {
    var bucket int

    startInd := segNum * hintSize

    symbol := make([]int, 256)

    for i := 0; i < 256; i++ {
        symbol[i] = -1
    }

    histBuf := make([]int, hintSize)

    for i := 0; i < hintSize; i++ {
        histBuf[i] = 0
    }

    for i := 0; i < hintSize; i++ {
        curByte := data[startInd + i]

        useBucket := symbol[curByte]
        if useBucket == -1 {
            symbol[curByte] = bucket
            useBucket = bucket
            bucket++
        }

        histBuf[useBucket]++
    }

    return histBuf, bucket
}

//
// Return the entropy of the data at segment indicated by segNum
//
func getEntropy(histBuf []int, coreSet int) float64 {
    logSampleSize := logLut[hintSize]

    entr := 0.0

    for ind := 0; ind < coreSet; ind++ {
        thisVal := histBuf[ind]
        thisValF := float64(thisVal)
        logDiff := logLut[thisVal] - logSampleSize
        entr += (thisValF * logDiff)
    }

    retEntr := (-1) * entr / hintSize

    return retEntr
}

//
// Given a page of data, predict whether the data is compressible or not.
// Returns true if the data is compressible. False otherwise.
//
func comprPredict(data []byte) (bool, []float64) {
    var segNum int

    retEntr := make([]float64, numHintsToTry)

    hintArr := initHintArr()

    succ := 0

    for i := 0; i < numHintsToTry; i++ {
        segNum, hintArr = getRandomSeg(hintArr)

        histBuf, coreSet := genHist(data, segNum)

        entr := getEntropy(histBuf, coreSet)

        retEntr[i] = entr

        if entr < entrThreshold {
            succ++
        }
    }

    if succ > 1 {
        return true, retEntr
    }

    return false, retEntr
}

//
// Given a page of data, compress it, and return the compressed length and
// an error indication. The compressed data is thrown away.
//
func comprPage(data []byte) (int, error) {
    var wBuf bytes.Buffer
    var rBuf bytes.Buffer
    var zw *gzip.Writer
    var zr *gzip.Reader
    var retLen int

    inLen := len(data)

    zw = gzip.NewWriter(&wBuf)

    _, err := zw.Write(data)
    if err != nil {
        return 0, err
    }

    if err := zw.Close(); err != nil {
        return 0, err
    }

    retLen = wBuf.Len()

    zr, err = gzip.NewReader(&wBuf)
    if err != nil {
        panic(err)
    }

    count, err := io.Copy(&rBuf, zr)
    checkError(err, "Decompress error")

    if count != int64(inLen) {
        fmt.Printf("count: %d inLen: %d\n", count, inLen)
        panic("decompression count")
    }

    decompSlice := rBuf.Bytes()

    for i := 0; i < inLen; i++ {
        if data[i] != decompSlice[i] {
            fmt.Printf("i: %d data[i]: %d decompSlice[i]: %d\n", i, data[i],
                                decompSlice[i])
            panic("data mismatch")
        }
    }

    return retLen, err
}

//
// Calculate the ideal entropy
//
func calcIdealEntropy() {

    defChars := make([]byte, hintSize)

    for ind := 0; ind < hintSize; ind++ {
        defChars[ind] = byte(ind)
    }

    histBuf, coreSet := genHist(defChars, 0)

    retEntr := getEntropy(histBuf, coreSet)

    fmt.Printf("Ideal entropy: %v\n", retEntr)
}

//
// Do various initializations
//
func doInit() {
    initLogLut()

    rand.Seed(time.Now().Unix())

    calcIdealEntropy()
}

func printPredict(retEntr []float64, predict bool, compressible bool,
                        comprCount int, goodPredict int64, badPredict int64) {
    fmt.Printf("Predict: %v compressible: %v comprCount: %d\n", predict,
                    compressible, comprCount)
    fmt.Printf("goodPredict: %v badPredict: %v entr0: %v entr1: %v\n",
                    goodPredict, badPredict, retEntr[0], retEntr[1])
    fmt.Printf("entr2: %v\n", retEntr[2])
}

func main() {
    var fName1 string
    var goodPredict, badPredict int64
    var pageCount int64
    var compressible bool

    startTime := time.Now()

    fmt.Printf("Start time: %v\n", startTime)

    doInit()

    fName1 = os.Args[1]

    fId1, err := os.Open(fName1)
    checkError(err, "Open error")

    data := make([]byte, pageSize)

    for ; ; {
        count, err := fId1.Read(data)
        if count == 0 || err == io.EOF {
            break;
        }
        checkError(err, "Read error")

        comprCount, err := comprPage(data)
        checkError(err, "comprPage error")

        if comprCount <= (pageSize / 2) {
            compressible = true
        } else {
            compressible = false
        }

        predict, retEntr := comprPredict(data)

        doPrint := false

        if predict == true {
            if compressible == true {
                goodPredict++
            } else {
                badPredict++
                doPrint = true
            }
        } else {
            if compressible == true {
                badPredict++
                doPrint = true
            } else {
                goodPredict++
            }
        }

        if doPrint == true {
            printPredict(retEntr, predict, compressible, comprCount,
                                    goodPredict, badPredict)
        }

        pageCount++

        if pageCount >= 1000 {
            break
        }
    }

    fmt.Printf("PageCount: %d GoodPredict: %d BadPredict: %d\n", pageCount,
                            goodPredict, badPredict)

    endTime := time.Now()

    fmt.Printf("End time: %v\n", time.Now())
    fmt.Printf("Elapsed time: %v\n", endTime.Sub(startTime))
}
