package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"strings"
)

// ---- main go routine ----
func main() {
	// what file do I want to zip
	fn := "test.csv"

	// open the file
	f, err := os.Open(fn)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// compress the file's contents
	data, err := Compress(f)
	if err != nil {
		panic(err)
	}

	// let's get a proper extension and maybe a cup of tea
	gz, err := ChangeExt(fn, "gz")
	if err != nil {
		panic(err)
	}

	// simulation s3 write ... this is where you write to S3 but I am doing it locally
	cf, err := os.Create(gz)
	if err != nil {
		panic(err)
	}
	defer cf.Close()
	w := gzip.NewWriter(cf)
	w.Write(data)
}

// ---- MyCompressor Interface ----
// should be placed in its own package ... leaving it here for challenge convenience
type MyCompressor interface {
	ChangeExt(fn string, ext string) (string, error)
	Compress(rc io.ReadCloser) ([]byte, error)
}

// ---- Compress ----
func Compress(rc io.ReadCloser) ([]byte, error) {
	// read the contents of the file
	reader := bufio.NewReader(rc)
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	// convert read bytes to string
	str := string(data)

	// create a byte buffer
	var b bytes.Buffer

	// Note: could have chosen gzip.NewWriterLevel for BestSpeed or BestCompression
	// The instructions made it clear that we wanted BestCompression but
	// BestSpeed may be better on low memory hardware.  So I just went with
	// a gzip.NewWriter instead.

	// zip the string to a byte buffer
	gz := gzip.NewWriter(&b)
	_, err = gz.Write([]byte(str))
	if err != nil {
		return nil, err
	}

	// close the gzip writer
	gz.Close()

	// return the compressed bytes for storage
	return b.Bytes(), nil
}

// ---- ChangeExt ----
// a handy routine to change a files extension based upon finding the last
// dot (.) in the file name thus assuming the remainder is an extension.
// The technical debt with this routine is that test.tony.file fails in that
// .file is not an extension.  Thought it important to point that out.
func ChangeExt(fn string, ext string) (string, error) {
	// throw error if missing fn is empty
	if len(fn) == 0 {
		return "", errors.New("filename not specified")
	}
	// throw error if ext is empty
	if len(ext) == 0 {
		return "", errors.New("ext not specified")
	}

	//  where's the dot (.)
	idx := strings.LastIndex(fn, ".")

	// dot was found, then ...
	if idx > -1 {
		// ... give me the filename up to but not including the dot
		fn = fn[0:idx]
	}

	// please note: if no dot was found then you are asking this routine
	// to suffix an extension to a file that does really have one

	// suffix the extension
	return fn + "." + ext, nil
}
