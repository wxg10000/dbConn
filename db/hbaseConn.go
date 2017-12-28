package db

import (
	"hbase"
	"fmt"
	"reflect"
	"git.apache.org/thrift.git/lib/go/thrift"
	"strconv"
	"time"
	"encoding/binary"
)

const HOST = "hbase1"
const PORT = "9090"
const TESTRECORD = 10

var client *hbase.THBaseServiceClient

type HBase struct{}

func init() {
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	transport, err := thrift.NewTSocket(HOST + ":" + PORT)
	if err != nil {
		panic(err)
	}
	client = hbase.NewTHBaseServiceClientFactory(transport, protocolFactory)
	if err := transport.Open(); err != nil {
		panic(err)
	}
	fmt.Println("Connection is ok")
	//defer transport.Close()
}

func (hb *HBase) IsExists(table string, rowkey string) (bool, error) {
	isexists, err := client.Exists([]byte(table), &hbase.TGet{Row: []byte(rowkey)})
	return isexists, err
}

func (hb *HBase) CreateTab(table string, family string) {

}

/*
插入单条数据
 */
func (hb *HBase) Insert(table string, family string, rowkey string, qualifier string, value string) {
	cvarr := []*hbase.TColumnValue{
		{
			Family:    []byte(family),
			Qualifier: []byte(qualifier),
			Value:     []byte(value),
		},
	}
	temptput := hbase.TPut{Row: []byte(rowkey), ColumnValues: cvarr}
	err := client.Put([]byte(table), &temptput)
	if err != nil {
		fmt.Printf("Put err:%s\n", err)
	} else {
		fmt.Println("Put done")
	}
}

/*
查询单条数据
 */
func (hb *HBase) Select(table string, rowkey string) {
	result, err := client.Get([]byte(table), &hbase.TGet{Row: []byte(rowkey)})
	if err != nil {
		fmt.Printf("Get err:%s\n", err)
	} else {
		fmt.Println("Rowkey:" + string(result.Row))
		for _, cv := range result.ColumnValues {
			printscruct(cv)
		}
	}
}

/*
修改单条数据
 */
func (hb *HBase) Update(table string, rowkey string, family string, qulifier string, update_value string) {
	cvarr := []*hbase.TColumnValue{
		{
			Family:    []byte(family),
			Qualifier: []byte(qulifier),
			Value:     []byte(update_value),
		},
	}
	temptput := hbase.TPut{Row: []byte(rowkey), ColumnValues: cvarr}
	err := client.Put([]byte(table), &temptput)
	if err != nil {
		fmt.Printf("Put update err:%s\n", err)
	} else {
		fmt.Println("Put update done")
	}
}

/*
删除单条数据
 */
func (hb *HBase) Delete(table string, rowkey string) {
	tdelete := hbase.TDelete{Row: []byte(rowkey)}
	err := client.DeleteSingle([]byte(table), &tdelete)
	if err != nil {
		fmt.Printf("DeleteSingle err:%s\n", err)
	} else {
		fmt.Print("DeleteSingel done\n")
	}
}

/*
插入多条数据
 */
func (hb *HBase) Inserts(table string, family string, qualifier string) {
	var tputArr []*hbase.TPut
	for i := 0; i < TESTRECORD; i++ {
		putrowkey := strconv.Itoa(i)
		tputArr = append(tputArr, &hbase.TPut{
			Row: []byte(putrowkey),
			ColumnValues: []*hbase.TColumnValue{
				{
					Family:    []byte(family),
					Qualifier: []byte(qualifier),
					Value:     []byte(time.Now().String()),
				},
			}})
	}
	err := client.PutMultiple([]byte(table), tputArr)
	if err != nil {
		fmt.Printf("PutMultiple err:%s\n", err)
	} else {
		fmt.Print("PutMultiple done\n")
	}
}

/*
获取多条数据
 */
func (hb *HBase) Selects(table string) {
	var tgets []*hbase.TGet
	for i := 0; i < TESTRECORD; i++ {
		putrowkey := strconv.Itoa(i)
		tgets = append(tgets, &hbase.TGet{
			Row: []byte(putrowkey)})
	}
	results, err := client.GetMultiple([]byte(table), tgets)
	if err != nil {
		fmt.Printf("GetMultiple err:%s", err)
	} else {
		fmt.Printf("GetMultiple Count:%d\n", len(results))
		for _, k := range results {
			fmt.Println("Rowkey:" + string(k.Row))
			for _, cv := range k.ColumnValues {
				printscruct(cv)
			}
		}
	}
}

/*
调用OpenScanner方法
 */
func (hb *HBase) OpenScanner(table string,family string,qualifier string) {
	startrow := make([]byte, 4)
	binary.LittleEndian.PutUint32(startrow, 1)
	stoprow := make([]byte, 4)
	binary.LittleEndian.PutUint32(stoprow, 10)
	scanresultnum, err := client.OpenScanner([]byte(table), &hbase.TScan{
		StartRow: startrow,
		StopRow:  stoprow,
		// FilterString: []byte("RowFilter(=, 'regexstring:00[1-3]00')"),
		// FilterString: []byte("PrefixFilter('1407658495588-')"),
		Columns: []*hbase.TColumn{
			{
				Family:    []byte(family),
				Qualifier: []byte(qualifier),
			},
		},
	})
	if err != nil {
		fmt.Printf("OpenScanner err:%s\n", err)
	} else {
		fmt.Printf("OpenScanner %d done\n", scanresultnum)
		scanresult, err := client.GetScannerRows(scanresultnum, 100)
		if err != nil {
			fmt.Printf("GetScannerRows err:%s\n", err)
		} else {
			fmt.Printf("GetScannerRows %d done\n", len(scanresult))
			for _, k := range scanresult {
				fmt.Println("scan Rowkey:" + string(k.Row))
				for _, cv := range k.ColumnValues {
					printscruct(cv)
				}
			}
		}
	}
}

/*
调用closescanner的方法
 */
func (hb *HBase) CloseScanner(scanresultnum int32) {
	err := client.CloseScanner(scanresultnum)
	if err != nil {
		fmt.Printf("CloseScanner err:%s\n", err)
	}
}
/*
调用GetScannerResults方法
 */
func (hb *HBase) GetScannerResults(table string,family string,startrow []byte,stoprow []byte) {
	gsr, err := client.GetScannerResults([]byte(table), &hbase.TScan{
		StartRow: startrow,
		StopRow:  stoprow,
		// FilterString: []byte("RowFilter(=, 'regexstring:00[1-3]00')"),
		// FilterString: []byte("PrefixFilter('1407658495588-')"),
		Columns: []*hbase.TColumn{
			{
				Family:    []byte(family),
				Qualifier: []byte("idoall.org"),
			},
		}}, 100)
	if err != nil {
		fmt.Printf("GetScannerResults err:%s\n", err)
	} else {
		fmt.Printf("GetScannerResults %d done\n", len(gsr))
		for _, k := range gsr {
			fmt.Println("scan Rowkey:" + string(k.Row))
			for _, cv := range k.ColumnValues {
				printscruct(cv)
			}
		}
	}
}

func (hb *HBase)Deletes(table string){
	var tdelArr []*hbase.TDelete
	for i := 0; i < TESTRECORD; i++ {
		putrowkey := strconv.Itoa(i)
		tdelArr = append(tdelArr, &hbase.TDelete{
			Row: []byte(putrowkey)})
	}
	r, err := client.DeleteMultiple([]byte(table), tdelArr)
	if err != nil {
		fmt.Printf("DeleteMultiple err:%s\n", err)
	} else {
		fmt.Printf("DeleteMultiple %d done\n", TESTRECORD)
		fmt.Println(r)
	}
}
func printscruct(cv interface{}) {
	switch reflect.ValueOf(cv).Interface().(type) {
	case *hbase.TColumnValue:
		s := reflect.ValueOf(cv).Elem()
		typeOfT := s.Type()
		//获取Thrift2中struct的field
		for i := 0; i < s.NumField(); i++ {
			f := s.Field(i)
			fileldformatstr := "\t%d: %s(%s)= %v\n"
			switch f.Interface().(type) {
			case []uint8:
				fmt.Printf(fileldformatstr, i, typeOfT.Field(i).Name, f.Type(), string(f.Interface().([]uint8)))
			case *int64:
				var tempint64 int64
				if f.Interface().(*int64) == nil {
					tempint64 = 0
				} else {
					tempint64 = *f.Interface().(*int64)
				}
				fmt.Printf(fileldformatstr, i, typeOfT.Field(i).Name, f.Type(), tempint64)
			default:
				fmt.Print("I don't know")
			}
		}
	default:
		fmt.Print("I don't know")
		fmt.Print(reflect.ValueOf(cv))
	}
}
