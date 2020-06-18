package fileutil

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/pinpt/go-common/v10/hash"
)

// Resolve with resolve a relative file path
func Resolve(filename string) (string, error) {
	if filename == "" {
		return filename, fmt.Errorf("filename was not supplied")
	}
	f, err := filepath.Abs(filename)
	if err != nil {
		return filename, err
	}
	_, err = os.Stat(f)
	if os.IsNotExist(err) {
		return f, err
	}
	return f, err
}

// FileExists returns true if the path components exist
func FileExists(filename ...string) bool {
	fn := filepath.Join(filename...)
	_, err := Resolve(fn)
	if err != nil {
		return false
	}
	return true
}

// ResolveFileName will attempt to resolve a filename attempting to look at both the basic name or the name + .gz extension
func ResolveFileName(fn string) string {
	if !FileExists(fn) {
		nfn := fn + ".gz"
		if FileExists(nfn) {
			return nfn
		}
	}
	return fn
}

type combinedReader struct {
	f io.ReadCloser
	g *gzip.Reader
}

func (c *combinedReader) Read(p []byte) (int, error) {
	return c.g.Read(p)
}

func (c *combinedReader) Close() error {
	c.g.Close()
	return c.f.Close()
}

// OpenFile will open a file and if it's gzipped return a gzipped reader as io.ReadCloser but will close both streams on close
func OpenFile(fn string) (io.ReadCloser, error) {
	f, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	var result io.ReadCloser = f
	if filepath.Ext(fn) == ".gz" {
		r, err := gzip.NewReader(f)
		if err != nil {
			f.Close()
			return nil, err
		}
		result = &combinedReader{f, r}
	}
	return result, nil
}

// FindFiles helper
func FindFiles(dir string, pattern *regexp.Regexp) ([]string, error) {
	fileList := []string{}
	err := filepath.Walk(dir, func(p string, f os.FileInfo, err error) error {
		// fmt.Println("trying to match", pattern, "for", path)
		if pattern.MatchString(filepath.Base(p)) && !strings.Contains(p, "gitdata") {
			fileList = append(fileList, p)
		}
		return nil
	})
	return fileList, err
}

// IsZeroLengthFile returns true if the filename specified is empty (0 bytes)
func IsZeroLengthFile(fn string) bool {
	f, err := os.Open(fn)
	defer f.Close()
	if err != nil {
		return true
	}
	s, err := f.Stat()
	if err != nil {
		return true
	}
	return s.Size() == 0
}

// ZipDir will zip create a zip archive named filename and fill it with files that
// match the pattern from dir
func ZipDir(filename string, dir string, pattern *regexp.Regexp) (int, error) {
	filenames, err := FindFiles(dir, pattern)
	if err != nil {
		return 0, err
	}
	newZipFile, err := os.Create(filename)
	if err != nil {
		return 0, err
	}
	defer newZipFile.Close()

	zipWriter := zip.NewWriter(newZipFile)
	defer zipWriter.Close()

	for _, file := range filenames {
		stat, _ := os.Stat(file)
		if !stat.IsDir() {
			if err = AddFileToZip(zipWriter, dir, file); err != nil {
				return 0, err
			}
		}
	}
	return len(filenames), nil
}

// AddFileToZip will add a file to a zip (writer), dir is the directory inside
// the zip archive that you want the file to exist in. If dir is `""`, then
// the file will be in the root of the archive
func AddFileToZip(zipWriter *zip.Writer, dir string, filename string) error {

	fileToZip, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer fileToZip.Close()

	// Get the file information
	info, err := fileToZip.Stat()
	if err != nil {
		return err
	}

	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return err
	}

	// Using FileInfoHeader() above only uses the basename of the file. If we want
	// to preserve the folder structure we can overwrite this with the full path.
	header.Name, _ = filepath.Rel(dir, filename)

	// Change to deflate to gain better compression
	// see http://golang.org/pkg/archive/zip/#pkg-constants
	header.Method = zip.Deflate

	writer, err := zipWriter.CreateHeader(header)
	if err != nil {
		return err
	}
	_, err = io.Copy(writer, fileToZip)
	return err
}

// Checksum will return the sha256 checksum of a file
func Checksum(fn string) (string, error) {
	of, err := os.Open(fn)
	if err != nil {
		return "", err
	}
	sum, err := hash.Sha256Checksum(of)
	if err != nil {
		return "", err
	}
	of.Close()
	sha := hex.EncodeToString(sum)
	return sha, nil
}

// ShaFiles will calculate the checksum for all files in dir that match re
// and write them out to outfile. Useful for code releases.
func ShaFiles(dir string, outfile string, re *regexp.Regexp) error {
	filenames, err := FindFiles(dir, re)
	if err != nil {
		return err
	}
	var shas strings.Builder
	for _, fn := range filenames {
		stat, _ := os.Stat(fn)
		if !stat.IsDir() {
			sha, err := Checksum(fn)
			if err != nil {
				return err
			}
			relfn, _ := filepath.Rel(dir, fn)
			shas.WriteString(sha + "  " + relfn + "\n")
		}
	}
	return ioutil.WriteFile(outfile, []byte(shas.String()), 0644)
}

// OpenNestedZip opens zip archive parent and searches it for child, then opens child and returns a zip.Reader.
// Useful for zip files inside zip files
func OpenNestedZip(parent, child string) (*zip.Reader, error) {
	reader, err := zip.OpenReader(parent)
	if err != nil {
		return nil, fmt.Errorf("error openning zip file: %w", err)
	}
	defer reader.Close()
	for _, file := range reader.File {
		if child == file.Name {
			f, err := file.Open()
			if err != nil {
				return nil, fmt.Errorf("error openning nested zip file (%s): %w", file.Name, err)
			}
			buf, err := ioutil.ReadAll(f)
			if err != nil {
				f.Close()
				return nil, fmt.Errorf("error reading nested zip file: %w", err)
			}
			f.Close()
			return zip.NewReader(bytes.NewReader(buf), file.FileInfo().Size())
		}
	}
	return nil, fmt.Errorf("cannot find zip file (%s) inside reader", child)
}

// Unzip will unzip archive src into folder dest
func Unzip(src, dest string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer r.Close()
	for _, f := range r.File {
		// Store filename/path for returning and using later on
		/* #nosec */
		fpath := filepath.Join(dest, f.Name)
		// Check for ZipSlip. More Info: http://bit.ly/2MsjAWE
		if !strings.HasPrefix(fpath, filepath.Clean(dest)+string(os.PathSeparator)) {
			return fmt.Errorf("%s: illegal file path", fpath)
		}
		if f.FileInfo().IsDir() {
			// Make Folder
			os.MkdirAll(fpath, os.ModePerm)
			continue
		}
		// Make File
		if err = os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
			return err
		}
		outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return err
		}
		rc, err := f.Open()
		if err != nil {
			return err
		}
		_, err = io.Copy(outFile, rc)
		// Close the file without defer to close before next iteration of loop
		outFile.Close()
		rc.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
