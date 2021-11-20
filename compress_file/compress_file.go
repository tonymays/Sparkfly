package main

import (
	"compress/gzip"
	"fmt"
	"io"
)

func main() {
	fmt.Println("Hello World")
}


func NewGzipReader(source io.ReadCloser) io.Reader {
	r, w := io.Pipe()
	go func() {
		defer w.Close()

		zip, err := gzip.NewWriterLevel(w, gzip.BestSpeed)
		defer zip.Close()
		if err != nil {
			w.CloseWithError(err)
		}

		io.Copy(zip, source)
	}()
	return r
}
