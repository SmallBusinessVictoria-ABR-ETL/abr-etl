package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"regexp"
	"sync"
	"syscall"
	"time"
)

func DataKey(s string, filenameDate time.Time) *string {
	return aws.String(fmt.Sprintf("/DATA/%s/importdate=%s/ABR_%s.txt.gz", s, filenameDate.Format("2006-01-02"), s))
}

func GetFile(s string) io.ReadSeeker {
	f := path.Join(os.Getenv("BASE_PATH"), s)
	err := exec.Command("/bin/gzip", f).Run()
	if err != nil {
		logs <- err.Error()
		return nil
	}
	file, err := os.Open(f + ".gz")
	if err != nil {
		logs <- err.Error()
		return nil
	}
	return file
}

func SyncToDataLake() {

	dir, err := ioutil.ReadDir(os.Getenv("BASE_PATH"))
	if err != nil {
		logs <- err.Error()
	}

	fileMap := map[string]*regexp.Regexp{}
	fileMap["ACNC"] = regexp.MustCompile("VIC([0-9]{6})_ABR_ACNC.txt$")
	fileMap["Agency_Data"] = regexp.MustCompile("VIC([0-9]{6})_ABR_Agency_Data.txt$")
	fileMap["Associates"] = regexp.MustCompile("VIC([0-9]{6})_ABR_Associates.txt$")
	fileMap["Businesslocation"] = regexp.MustCompile("VIC([0-9]{6})_ABR_Businesslocation.txt$")
	fileMap["Businessname"] = regexp.MustCompile("VIC([0-9]{6})_ABR_Businessname.txt$")
	fileMap["Funds"] = regexp.MustCompile("VIC([0-9]{6})_ABR_Funds.txt$")
	fileMap["Othtrdnames"] = regexp.MustCompile("VIC([0-9]{6})_ABR_Othtrdnames.txt$")
	fileMap["Replacedabn"] = regexp.MustCompile("VIC([0-9]{6})_ABR_Replacedabn.txt$")

	postUploadActions := map[string][]func(){}
	postUploadActions["Agency_Data"] = []func(){AgencyDelta}

	m, _ := time.LoadLocation("Australia/Melbourne")

	wg := sync.WaitGroup{}
	for _, file := range dir {
		for key, reg := range fileMap {
			if reg.MatchString(file.Name()) {
				filenameDate, err := time.ParseInLocation("060102", reg.FindStringSubmatch(file.Name())[1], m)
				if err != nil {
					logs <- err.Error()
				}

				wg.Add(1)
				go func(k, f string, t time.Time) {
					S3Sync(k, f, t)
					// Clean up after syncing
					err = syscall.Unlink(path.Join(os.Getenv("BASE_PATH"), f))
					if err != nil {
						logs <- err.Error()
					}
					if pua, ok := postUploadActions[k]; ok {
						for _, a := range pua {
							a()
						}
					}

					wg.Done()
				}(key, file.Name(), filenameDate)
			}
		}
	}
	wg.Wait()

}

func S3Sync(s string, file string, filenameDate time.Time) {

	logs <- "Uploading " + file + " to " + *DataKey(s, filenameDate)
	f := GetFile(file)
	if f == nil {
		return
	}
	s3c := s3.New(session.Must(session.NewSession()))
	resp, err := s3c.PutObject(&s3.PutObjectInput{
		Body:   f,
		Bucket: aws.String(os.Getenv("S3_DATA_BUCKET")),
		Key:    DataKey(s, filenameDate),
	})
	if err != nil {
		logs <- err.Error()
		return
	}
	logs <- resp.String()
	if fc, ok := f.(io.Closer); ok {
		_ = fc.Close()
	}
}
