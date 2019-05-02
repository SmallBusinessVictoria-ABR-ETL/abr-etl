package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/s3"
	"io/ioutil"
	"log"
	"os"
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
	logs <- "Starting ABR ETL Process"

	// Check to see if ABR ETL process should run or not
	checkDisabledStatus()

	// Skip download for testing
	if os.Getenv("SKIP_DOWNLOAD") == "" {
		DownloadABRData()
	}

	// Extract files from .zip
	UnzipABRData()

	// Copy Files to S3 (and Run Deltas)
	SyncToDataLake()

	// Remove temporary files from ETL Server
	CleanUp()
}

func init() {

	// Set up CloudWatch Loging
	cwl = cloudwatchlogs.New(session.Must(session.NewSession()))
	logGroupName = aws.String(os.Getenv("CW_GROUP"))
	logs = make(chan string)
	go logEvent(logs)
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
