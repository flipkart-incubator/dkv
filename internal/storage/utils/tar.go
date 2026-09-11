package utils

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
)

type StreamingTar struct {
	sourceFiles     []*os.File
	readDataIndex   int64
	readHeaderIndex int
	sourceIndex     int
	headers         []*tar.Header
	buf             bytes.Buffer
	tw              *tar.Writer
}

// A tar file is a collection of binary data segments (usually sourced from files).
// Each segment starts with a header that contains metadata about the binary data, that follows it, and how to reconstruct it as a file.
// +---------------------------+
//| [name][mode][uid][guild]  |
//| ...                       |
//+---------------------------+
//| XXXXXXXXXXXXXXXXXXXXXXXXX |
//| XXXXXXXXXXXXXXXXXXXXXXXXX |
//| XXXXXXXXXXXXXXXXXXXXXXXXX |
//+---------------------------+
//| [name][mode][uid][guild]  |
//| ...                       |
//+---------------------------+
//| XXXXXXXXXXXXXXXXXXXXXXXXX |
//| XXXXXXXXXXXXXXXXXXXXXXXXX |
//+---------------------------+

// CreateStreamingTar returns a tar io.Reader while streaming directly from the source files.
func CreateStreamingTar(sources ...*os.File) (*StreamingTar, error) {
	if len(sources) == 0 {
		return nil, errors.New("no source files")
	}
	tarFile := StreamingTar{
		sourceFiles:     sources,
		readDataIndex:   0,
		readHeaderIndex: -1,
		sourceIndex:     -1,
		headers:         make([]*tar.Header, len(sources)),
	}
	tarFile.tw = tar.NewWriter(&tarFile.buf)

	for i, f := range sources {
		stat, err := f.Stat()
		if err != nil {
			return nil, err
		}

		fileName := filepath.Base(f.Name())
		//header.Name = strings.TrimPrefix(path, source) //we don't want any folder-name prefix

		tarFile.headers[i] = &tar.Header{
			Name: fileName,
			Mode: 0644,
			Size: stat.Size(),
		}
	}
	return &tarFile, nil
}

func (r *StreamingTar) readHeader(p []byte) (n int, err error) {
	if len(p) < r.buf.Len() {
		r.readHeaderIndex += len(p)
		return r.buf.Read(p)
	} else {
		r.readHeaderIndex = -1
		return r.buf.Read(p)
	}
}

func (r *StreamingTar) Close() error {
	for _, f := range r.sourceFiles {
		f.Close()
	}
	return nil
}

func (r *StreamingTar) Read(p []byte) (n int, err error) {

	if r.sourceIndex == -1 {
		//start of reading
		r.sourceIndex++
		r.tw.WriteHeader(r.headers[r.sourceIndex])
		r.readHeaderIndex = 0
		return r.readHeader(p)
	}

	//complete writing the header first
	if r.readHeaderIndex != -1 {
		return r.readHeader(p)
	}

	if r.readDataIndex < r.headers[r.sourceIndex].Size {
		//write this file.
		buf := make([]byte, len(p))
		nbytes, err := r.sourceFiles[r.sourceIndex].Read(buf)
		if err == io.EOF {
			//this is okay
		}
		r.readDataIndex += int64(nbytes)
		r.tw.Write(buf)
		return r.buf.Read(p)
	}

	//check if there is anything pending in the buf anymore.
	//This could be possible if there was some padding bytes that was added when the previous filed ended.
	if r.buf.Len() > 0 {
		return r.buf.Read(p)
	}

	// reached here means either current file is over
	r.sourceIndex++
	//check if all files are over.
	if r.sourceIndex >= len(r.sourceFiles) {
		err = io.EOF
		return
	}

	// otherwise write next file header
	r.tw.WriteHeader(r.headers[r.sourceIndex])
	r.readHeaderIndex = 0
	r.readDataIndex = 0
	return r.readHeader(p)

}

// CreateInMemoryTar creates a tar file .
func CreateInMemoryTar(sources ...*os.File) ([]byte, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	defer tw.Close()
	for _, f := range sources {
		stat, err := f.Stat()
		if err != nil {
			return nil, err
		}
		hdr := &tar.Header{
			Name: filepath.Base(f.Name()),
			Mode: 0644,
			Size: stat.Size(),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return nil, err
		}

		if _, err := io.Copy(tw, f); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil

}

func ExtractTar(tarF io.Reader, target string) (int, error) {
	tarReader := tar.NewReader(tarF)
	numFiles := 0
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return numFiles, err
		}

		path := filepath.Join(target, header.Name)
		info := header.FileInfo()
		if info.IsDir() {
			if err = os.MkdirAll(path, info.Mode()); err != nil {
				return numFiles, err
			}
			continue
		}

		file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
		if err != nil {
			return numFiles, err
		}
		_, err = io.Copy(file, tarReader)
		if err != nil {
			return numFiles, err
		}
		numFiles++
		_ = file.Close()
	}
	return numFiles, nil
}
