package internal

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
)

const (
	maxAttempts    = 4
	maxConcurrents = 64
)

var sem = make(chan struct{}, maxConcurrents)

func BulkDownload(list []string, output string) error {
	var errFlag bool
	var wg sync.WaitGroup

	for i, v := range list {
		wg.Add(1)
		// Name files by their index in the list, not by the url.
		// The files are concatenated in name order, so this keeps
		// the order of the list whatever the segment urls look like.
		fileName := fmt.Sprintf("%09d.aac", i)
		go func(link, fileName string) {
			defer wg.Done()

			var err error
			for attempt := 0; attempt < maxAttempts; attempt++ {
				sem <- struct{}{}
				err = download(link, filepath.Join(output, fileName))
				<-sem
				if err == nil {
					break
				}
			}
			if err != nil {
				log.Printf("Failed to download: %s", err)
				errFlag = true
			}
		}(v, fileName)
	}
	wg.Wait()

	if errFlag {
		return errors.New("Lack of aac files")
	}
	return nil
}

func download(link, output string) error {
	resp, err := http.Get(link)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Do not save an error page as an aac file.
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download %s: status %d", link, resp.StatusCode)
	}

	file, err := os.Create(output)
	if err != nil {
		return err
	}

	_, err = io.Copy(file, resp.Body)
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	return err
}
