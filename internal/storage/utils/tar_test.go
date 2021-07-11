package utils

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const (
	letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	testDir     = "/tmp/tar_test"
)

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestTarCreate(t *testing.T) {
	os.MkdirAll(testDir, 0777)
	createAndVerifyTar(t, 1, 100)
	createAndVerifyTar(t, 2, 2000)
	createAndVerifyTar(t, 5, 5000)
	createAndVerifyTar(t, 2, 10000)
	createAndVerifyTar(t, 2, 202020)
	createAndVerifyTar(t, 2, 3876543)
	createAndVerifyTar(t, 2, 34567801)
}

func TestMemoryTarCreate(t *testing.T) {
	os.MkdirAll(testDir, 0777)
	createAndVerifyMemoryTar(t, 2, 2000)
	createAndVerifyMemoryTar(t, 5, 5000)
}

func createTmpFiles(t *testing.T, count int, size int) []*os.File {
	var files []*os.File
	for i := 0; i < count; i++ {
		f, err := ioutil.TempFile("/tmp", fmt.Sprintf("tar_test-%d_*.txt", i+1))
		if err != nil {
			t.Fatal(err)
		}

		minSize := int(float32(size) * 0.7)
		randomSize := rand.Intn(size - minSize + 1) + minSize
		token := make([]byte, randomSize)
		rand.Read(token)
		f.Write(token)
		f.Sync()
		f.Seek(0, 0)

		files = append(files, f)
	}
	return files
}

func createAndVerifyTar(t *testing.T, filesCount, size int) {
	files := createTmpFiles(t, filesCount, size)
	tarF, err := CreateStreamingTar(files...)
	if err != nil {
		t.Fatal(err)
	}
	verifyTar(t, tarF, files)
}

func createAndVerifyMemoryTar(t *testing.T, filesCount, size int) {
	files := createTmpFiles(t, filesCount, size)
	tarBytes, err := CreateInMemoryTar(files...)
	if err != nil {
		t.Fatal(err)
	}
	tarF := bytes.NewReader(tarBytes)
	verifyTar(t, tarF, files)
}

func verifyTar(t *testing.T, tarF io.Reader, files []*os.File) {
	//Read the tar file back
	i := 0
	tr := tar.NewReader(tarF)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		contents, err := ioutil.ReadAll(tr)
		if err != nil {
			log.Fatal(err)
		}
		t.Logf("Tared File %s Expected Len : %d , Actual Len:%d \n", hdr.Name, hdr.Size, len(contents))

		var refFile *os.File
		for _, f := range files {
			if filepath.Base(f.Name()) == hdr.Name {
				refFile = f
			}
		}

		if refFile == nil {
			t.Error("refFile cannot be nil")
		} else {
			fileContents, err := ioutil.ReadFile(refFile.Name())
			if err != nil {
				log.Fatal(err)
			}

			if string(fileContents) != string(contents) {
				t.Error("Text 1 didn't match in with original file")
			}
		}
		i++
	}

	if i != len(files) {
		t.Errorf("Failed to find expected no of files in tar. Expected %d Got %d", len(files), i)
	}
}
