package main

import (
	"fmt"
	"github.com/gkalele/s3tohdfs/backend/s3hadoop"
	"net/http"
	"time"

	"github.com/gkalele/s3tohdfs"
	"github.com/gkalele/s3tohdfs/backend/s3mem"
)

func main() {
	var faker *s3tohdfs.GoFakeS3
	if false {
		faker = s3tohdfs.New(s3mem.New())
	} else {
		faker = s3tohdfs.New(s3hadoop.Factory(s3hadoop.HdfsConfig{
			Namenode:    "namenode.service.consul",
			HdfsPort:    8020,
			WebHdfsPort: 50070,
			WebHdfsUser: "tetter",
		}))
	}
	if err := http.ListenAndServe("0.0.0.0:4000", faker.Server()); err != nil {
		panic(err.Error())
	}

	fmt.Println("Started S3 server")
	time.Sleep(30 * time.Second)
}
