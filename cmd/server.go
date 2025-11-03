package main

import (
	"fmt"

	"github.com/Ankit-1204/FlintDB.git/internals"
)

func main() {
	fmt.Println("DBname: ")
	var dbname string
	fmt.Scan(&dbname)
	db, err := internals.Open(dbname)
	if err != nil {
		fmt.Println(err)
		return
	}
	for {
		fmt.Println("give operation: ")
		var op string
		fmt.Scan(&op)
		if op == "G" {
			fmt.Println("give key and value: ")
			var (
				key string
			)
			fmt.Scan(&key)
			v := db.Get(key)
			fmt.Print(v)
		} else if op == "P" {
			fmt.Println("give key and value: ")
			var (
				key   string
				value string
			)
			fmt.Scan(&key, &value)
			valu := []byte(value)
			err := db.Put(key, valu)
			if err != nil {
				fmt.Print(err)
				return
			}
		}
	}

}
