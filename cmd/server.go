package main

import (
	"github.com/Ankit-1204/FlintDB.git/internals"
)

func start() error {
	internals.Start()
	for {
		select {}
	}
}
