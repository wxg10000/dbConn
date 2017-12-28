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

}



