package main

import (
	"encoding/base64"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/kms"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	cwl           *cloudwatchlogs.CloudWatchLogs
	logGroupName  *string
	logStreamName *string
	logs          chan string
)

func main() {

	cwl = cloudwatchlogs.New(session.Must(session.NewSession()))
	logGroupName = aws.String(os.Getenv("CW_GROUP"))
	logs = make(chan string)
	go logEvent(logs)
	logs <- "Starting ABR ETL Process"

	checkDisabledStatus()
	if os.Getenv("SKIP_DOWNLOAD") == "" {
		DownloadABRData()
	}
	UnzipABRData()
	SyncToDataLake()
	CleanUp()
}

const agencyChangeSQL = `
SELECT n.pid,
n.abn,
n.ent_typ_cd,
n.org_nm,
n.nm_titl_cd,
n.prsn_gvn_nm,
n.prsn_othr_gvn_nm,
n.prsn_fmly_nm,
n.nm_sufx_cd,
n.abn_regn_dt,
n.abn_cancn_dt,
n.mn_trdg_nm,
n.son_addr_ln_1,
n.son_addr_ln_2,
n.son_sbrb,
n.son_stt,
n.son_pc,
n.son_cntry_cd,
n.son_dpid,
n.mn_bus_addr_ln_1,
n.mn_bus_addr_ln_2,
n.mn_bus_sbrb,
n.mn_bus_stt,
n.mn_bus_pc,
n.mn_bus_cntry_cd,
n.mn_bus_dpid,
n.ent_eml,
n.prty_id_blnk,
n.gst_regn_dt,
n.gst_cancn_dt,
n.mn_indy_clsn,
n.mn_indy_clsn_descn,
n.acn,
n.sprsn_ind

FROM abr_weekly_agency_data n
JOIN abr_weekly_agency_data n2 on n.pid=n2.pid

WHERE n.importdate='{{.Arg1}}'
AND n2.importdate='{{.Arg2}}'

and (n.abn != n2.abn OR
n.ent_typ_cd != n2.ent_typ_cd OR
n.org_nm != n2.org_nm OR
n.nm_titl_cd != n2.nm_titl_cd OR
n.prsn_gvn_nm != n2.prsn_gvn_nm OR
n.prsn_othr_gvn_nm != n2.prsn_othr_gvn_nm OR
n.prsn_fmly_nm != n2.prsn_fmly_nm OR
n.nm_sufx_cd != n2.nm_sufx_cd OR
n.abn_regn_dt != n2.abn_regn_dt OR
n.abn_cancn_dt != n2.abn_cancn_dt OR
n.mn_trdg_nm != n2.mn_trdg_nm OR
n.son_addr_ln_1 != n2.son_addr_ln_1 OR
n.son_addr_ln_2 != n2.son_addr_ln_2 OR
n.son_sbrb != n2.son_sbrb OR
n.son_stt != n2.son_stt OR
n.son_pc != n2.son_pc OR
n.son_cntry_cd != n2.son_cntry_cd OR
n.son_dpid != n2.son_dpid OR
n.mn_bus_addr_ln_1 != n2.mn_bus_addr_ln_1 OR
n.mn_bus_addr_ln_2 != n2.mn_bus_addr_ln_2 OR
n.mn_bus_sbrb != n2.mn_bus_sbrb OR
n.mn_bus_stt != n2.mn_bus_stt OR
n.mn_bus_pc != n2.mn_bus_pc OR
n.mn_bus_cntry_cd != n2.mn_bus_cntry_cd OR
n.mn_bus_dpid != n2.mn_bus_dpid OR
n.ent_eml != n2.ent_eml OR
n.prty_id_blnk != n2.prty_id_blnk OR
n.gst_regn_dt != n2.gst_regn_dt OR
n.gst_cancn_dt != n2.gst_cancn_dt OR
n.mn_indy_clsn != n2.mn_indy_clsn OR
n.mn_indy_clsn_descn != n2.mn_indy_clsn_descn OR
n.acn != n2.acn OR
n.sprsn_ind != n2.sprsn_ind)
`

func Query(sql, location string) {
	athenaSession := athena.New(session.Must(session.NewSession()))

	resp, err := athenaSession.StartQueryExecution(&athena.StartQueryExecutionInput{
		QueryString: aws.String(sql),
		QueryExecutionContext: &athena.QueryExecutionContext{
			Database: aws.String("sbv_abr"),
		},
		ResultConfiguration: &athena.ResultConfiguration{
			OutputLocation: aws.String(location),
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	for true {
		r2, err := athenaSession.GetQueryExecution(&athena.GetQueryExecutionInput{
			QueryExecutionId: resp.QueryExecutionId,
		})
		if err != nil {
			log.Fatal(err)
		}

		log.Print("State: ", *r2.QueryExecution.Status.State)
		time.Sleep(time.Second)

		if *r2.QueryExecution.Status.State != "RUNNING" {
			log.Print(*r2.QueryExecution.Status)
			break
		}
		//
		//	log.Print(*r2.QueryExecution.Status)
		//
		//	//fmt.Print("\n\n-----\n" + location + "/" + *resp.QueryExecutionId + ".csv\n\n-----\n")
		//
		//	o, _ := s3Session.GetObjectRequest(ObjectRequestFromS3Url(location + "/" + *resp.QueryExecutionId + ".csv"))
		//
		//	signed, err := o.Presign(time.Hour * 24)
		//	if err != nil {
		//		log.Fatal("Failed to sign url")
		//	}
		//
		//	fmt.Print("\n---------------\n" + signed + "\n---------------\n")
		//	break
		//}

	}
}

func CleanUp() {
	err := syscall.Unlink("VIC_ABR_Extract.zip")
	if err != nil {
		logs <- err.Error()
	}
}

func checkDisabledStatus() {
	// s3://sbv-abr-etl/disabled
	s3c := s3.New(session.Must(session.NewSession()))
	resp, err := s3c.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(os.Getenv("S3_DATA_BUCKET")),
		Key:    aws.String("disabled"),
	})
	if err != nil {
		logs <- err.Error()
		return
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logs <- err.Error()
	}
	if string(b) == "disabled" {
		logs <- "ETL Disabled - s3://" + os.Getenv("S3_DATA_BUCKET") + "/disabled"
		os.Exit(1)
	}
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

func UnzipABRData() {
	unzip := exec.Command("/usr/bin/unzip", "-o", "VIC_ABR_Extract.zip")
	out, err := unzip.CombinedOutput()
	if err != nil {
		logs <- err.Error()
	}
	logs <- string(out)
}

func DownloadABRData() bool {
	logs <- ("Downloading ABR Data")

	user := Decrypt(os.Getenv("USER_ENC"))
	pass := Decrypt(os.Getenv("PASS_ENC"))
	logs <- ("User " + user)

	batch := "get AllStates_ABR\\ Data/Sent/VIC_ABR\\ Extract.zip VIC_ABR_Extract.zip"
	sftpProcess := exec.Command("/usr/bin/sshpass", "-p", pass, "/usr/bin/sftp", "-c", "aes256-cbc", "-o", "StrictHostKeyChecking=no", "-oKexAlgorithms=+diffie-hellman-group-exchange-sha256", user+"@180.149.195.60")
	sftpProcess.Stdin = strings.NewReader(batch)
	out, err := sftpProcess.CombinedOutput()
	if err != nil {
		logs <- err.Error()
		return false
	}
	logs <- string(out)
	logs <- "Download Complete"
	return true
}

func hide(s string) string {
	return strings.Repeat("•", len(s))
}

func Decrypt(s string) string {
	keys := kms.New(session.Must(session.NewSession()))
	reader := strings.NewReader(s)
	b64 := base64.NewDecoder(base64.StdEncoding, reader)
	CiphertextBlob, err := ioutil.ReadAll(b64)
	if err != nil {
		logs <- err.Error()
		return ""
	}
	decrypted, err := keys.Decrypt(&kms.DecryptInput{
		CiphertextBlob: CiphertextBlob,
	})
	if err != nil {
		logs <- err.Error()
		return ""
	}
	return string(decrypted.Plaintext)
}

func NowTZ() time.Time {
	m, _ := time.LoadLocation("Australia/Melbourne")
	return time.Now().In(m)
}

func logEvent(msgChan chan string) {

	logStreamName = aws.String(NowTZ().Format("2006-01-02T15-04-05Z0700"))
	_, err := cwl.CreateLogStream(&cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  logGroupName,
		LogStreamName: logStreamName,
	})
	if err != nil {
		log.Print("failed to create log stream ", err)
	}
	stream, _ := cwl.DescribeLogStreams(&cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName:        logGroupName,
		LogStreamNamePrefix: logStreamName,
	})
	t := stream.NextToken

	for {
		msg := <-msgChan
		log.Print(msg)
		event, err := cwl.PutLogEvents(&cloudwatchlogs.PutLogEventsInput{
			LogGroupName:  logGroupName,
			LogStreamName: logStreamName,
			SequenceToken: t,
			LogEvents: []*cloudwatchlogs.InputLogEvent{
				{Timestamp: aws.Int64(time.Now().Unix() * 1000),
					Message: &msg},
			},
		})
		if err != nil {
			log.Print(msg)
			log.Print(err)
		}
		//fmt.Print(event.RejectedLogEventsInfo.String())
		t = event.NextSequenceToken
	}
}
