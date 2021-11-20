/*
This solution uses waitgroups, mutexes, function closures, and the recover
method to gracefully recover from a panic issued when a duplicate is found.

This solution contains two methods: main() and proc(...)

The idea behind the method is to process every file encountered within its own
go routine.  Each file represented will share a single map to search for and store
unique codes.  A panic will be issued if a map search determines a code to be a
duplicate.  The panic will be handled by the recover() function which will allow
our main go routine to exit gracefully.

There are many ways to shutdown go routine processes - channels, context cancels, etc

So I thought it may be of entertainment value to illustrate how to gracefully
shutdown on a panic with the recover() method.  Hopefully, it is a unique
thought solution for this issue.
*/

package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
)

// ---- main go routine ----
func main() {
	// setup a waitgroup
	var wg sync.WaitGroup

	// init an empty map
	m := map[string]string{}

	// setup a mutex to lock map entries
	mux := sync.Mutex{}

	// where are our files ... CHANGE THIS TO MATCH YOUR TEST ENVIRONMENT
	dir := "/home/api_user/sparkfly/dupcode/code_files"

	// read the files and throw and any errors
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		// no graceful exit here Gandalf
		panic(err)
	}

	// let's talk a walk
	for _, file := range files {
		// increment the wait group counter
		wg.Add(1)

		// is this a file ... ignore otherwise
		if !file.IsDir() {
			// set the fullpath
			fullpath := dir + "/" + file.Name()

			// process the file on a go routine
			go proc(fullpath, m, &wg, &mux)
		}
	}

	// tell the main go routine to hold up for a moment
	wg.Wait()

	// well ... how did we do?
	fmt.Println("Process complete - no duplciates found")
}

// ---- proc ----
// takes the fullpath, the process map, a pointer to our wait group and a pointer to our mutex
func proc(fullpath string, m map[string]string, wg *sync.WaitGroup, mux *sync.Mutex) {
	// let's defer closing this wait group until the very end
	defer wg.Done()

	// setup and defer a func to properly handle a panic
	defer func() {
		// did we get an error, if so recover from it and gracefully exit
		if r := recover(); r != nil {
			// display the error we got
			fmt.Printf("%v\n", r)

			// let the world know we got this
			fmt.Println("cleaning up and exiting gracefully")

			// exit
			os.Exit(0)
		}
	}()

	// open the file
	f, err := os.Open(fullpath)

	// run our defer recover if we catch an error
	if err != nil {
		panic(err)
	}

	// open a scanner
	scanner := bufio.NewScanner(f)

	// bust it on the newline
	scanner.Split(bufio.ScanLines)

	// walk the contents like you are on the beach
	for scanner.Scan() {
		// split the line since it appears to be comma delimited
		s := strings.Split(scanner.Text(),",")

		// lose the file header record
		if s[0] != "barcode" {
			// place the lock to protect our map
			mux.Lock()

			// search the map for the code
			_, found := m[s[1]]

			// if we find it in the map ...
			if found {
				// stop everything right here with our customer error message
				panic("duplicate " + s[0] + "->" +s[1])
			// and if we did not find it
			} else {
				// add our code to the map
				m[s[1]] = s[1]
			}

			// awesome.  Let's unlock or map for other visitors
			mux.Unlock()
		}
	}
}
