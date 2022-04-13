package internal

import (
	"embed"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

//go:embed www
var embededFiles embed.FS

func getFileDir() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		file = "./"
	} else {
		file = filepath.Dir(file)
	}

	dir := fmt.Sprintf("%s/www", file)
	return dir
}

func GetWWWFileSystem(develMode bool) http.FileSystem {

	if develMode {
		dir := getFileDir()
		log.Print("using live mode with Dir: ", dir)
		return http.FS(os.DirFS(dir))
	}

	log.Println("using embed mode")
	fSys, err := fs.Sub(embededFiles, "www")
	if err != nil {
		panic(err)
	}

	//fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
	//	fmt.Println(p)
	//	return nil
	//})

	return http.FS(fSys)
}

var epoch = time.Unix(0, 0).Format(time.RFC1123)

var noCacheHeaders = map[string]string{
	"Expires":         epoch,
	"Cache-Control":   "no-cache, private, max-age=0",
	"Pragma":          "no-cache",
	"X-Accel-Expires": "0",
}

var etagHeaders = []string{
	"ETag",
	"If-Modified-Since",
	"If-Match",
	"If-None-Match",
	"If-Range",
	"If-Unmodified-Since",
}

func NoCache(h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		// Delete any ETag headers that may have been set
		for _, v := range etagHeaders {
			if r.Header.Get(v) != "" {
				r.Header.Del(v)
			}
		}

		// Set our NoCache headers
		for k, v := range noCacheHeaders {
			w.Header().Set(k, v)
		}

		h.ServeHTTP(w, r)
	}

	return http.HandlerFunc(fn)
}
