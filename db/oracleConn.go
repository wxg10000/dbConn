package db

import (
	_ "dbConn/go-oci8-master"
	"database/sql"
	"log"
)

type Oracle struct {
}

var db *sql.DB
var err error

func init() {
	db, err = sql.Open("oci8", "scott/manager@orcl")
	if err != nil {
		log.Fatal(err)
	}
}

/*
创建表格
 */
func (orcl *Oracle) Create(sqlstr string) {
	_, err := db.Exec(sqlstr)
	checkError(err)
}

/*
查询数据
 */
func (orcl *Oracle) Select(sqlstr string) {
	rows, err := db.Query(sqlstr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	for rows.Next() {
			var f1 float64
			var f2 string
			rows.Scan(&f1, &f2)
			log.Println(f1, f2) // 3.14 foo
	}
	rows.Close()
}

/*
插入一条数据
 */
func (orcl *Oracle) Insert(sqlstr string) {
	_, err := db.Exec(sqlstr)
	checkError(err)
}

/*
修改一条数据
 */
func (orcl *Oracle) Update(sqlstr string) {
	_, err := db.Exec(sqlstr)
	checkError(err)
}

/*
删除一条数据
 */
func (orcl *Oracle) Delete(sqlstr string) {
	_, err := db.Exec(sqlstr)
	checkError(err)
}

/*
删除表格
 */
func drop(sql string) {
	_, err := db.Exec(sql)
	checkError(err)
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
