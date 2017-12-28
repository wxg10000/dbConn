package main

import (
	. "dbConn/db"
	"fmt"
)
func main() {
	var hb HBase
	b,err :=hb.IsExists("e_test","1")
	fmt.Println(b,err)
	hb.Insert("e_test","f1","1","hello","helloworld")
	hb.Select("e_test","1")
	hb.Delete("e_test","1")
	hb.Inserts("e_test","f1","hello")
	hb.Selects("e_test")


	var db Oracle
	db.Select("select 3.14,'hello' from dual")

}

