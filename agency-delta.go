package main

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
	"os"
	"path"
	"sort"
	"strings"
	"time"
)

func AgencyDelta() {
	// Add New Partitions
	Query("MSCK REPAIR TABLE abr_weekly_agency_data", fmt.Sprintf("s3://%s/MSCK", os.Getenv("S3_DATA_BUCKET")))

	newest, previous, err := GetDeltaPartitions()

	if err != nil {
		logs <- err.Error()
		return
	}

	RunAgendyDataDelta(newest, previous)
}

func RunAgendyDataDelta(newest string, previous string) {
	// Find Records that changed
	logs <- "Running Delta Query (Change)"
	Query(fmt.Sprintf(agencyChangeSQL, newest, previous), fmt.Sprintf("s3://%s/DELTA/UPDATED/Agency_Data/importdate=%s/", os.Getenv("S3_DATA_BUCKET"), newest))
	RenameDelta(newest, "Agency_Data", "updated")

	// Find Records that were added
	logs <- "Running Delta Query (New)"
	Query(fmt.Sprintf(agencyNewSQL, newest, previous), fmt.Sprintf("s3://%s/DELTA/ADDED/Agency_Data/importdate=%s/", os.Getenv("S3_DATA_BUCKET"), newest))
	RenameDelta(newest, "Agency_Data", "added")
}

func RenameDelta(newest, dataSet, action string) {
	s3c := s3.New(session.Must(session.NewSession()))
	bucket := os.Getenv("S3_DATA_BUCKET")
	list, err := s3c.ListObjects(&s3.ListObjectsInput{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(fmt.Sprintf("DELTA/%s/%s/importdate=%s/", strings.ToUpper(action), dataSet, newest)),
		MaxKeys: aws.Int64(10000),
	})
	if err != nil {
		logs <- err.Error()
		return
	}
	for _, file := range list.Contents {
		if path.Ext(*file.Key) == ".csv" {
			if path.Base(*file.Key) == fmt.Sprintf("%s_%s.csv", dataSet, action) {
				continue
			}
			newKey := fmt.Sprintf("DELTA/%s/%s/importdate=%s/%s_%s.csv", strings.ToUpper(action), dataSet, newest, dataSet, action)
			logs <- "Delta:   " + *file.Key
			logs <- "Copy to: " + newKey

			err = RenameS3(bucket, *file.Key, newKey)
			if err != nil {
				logs <- err.Error()
				return
			}
		}
	}
}

func RenameS3(bucket, from, to string) error {
	s3c := s3.New(session.Must(session.NewSession()))
	copyResp, err := s3c.CopyObject(&s3.CopyObjectInput{
		Bucket:     &bucket,
		Key:        &to,
		CopySource: aws.String(fmt.Sprintf("%s/%s", bucket, from)),
	})
	if err != nil {
		return err
	}
	logs <- *copyResp.CopyObjectResult.ETag

	rmResp, err := s3c.DeleteObject(&s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &from,
	})
	if err != nil {
		return err
	}
	logs <- *rmResp.VersionId
	return nil
}

func GetDeltaPartitions() (string, string, error) {
	s3c := s3.New(session.Must(session.NewSession()))
	list, err := s3c.ListObjects(&s3.ListObjectsInput{
		Bucket:    aws.String(os.Getenv("S3_DATA_BUCKET")),
		Prefix:    aws.String("DATA/Agency_Data/"),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int64(10000),
	})
	if err != nil {
		return "", "", err
	}
	listUnordered := []string{}
	for _, date := range list.CommonPrefixes {
		b := strings.SplitN(path.Base(*date.Prefix), "=", 2)
		if len(b) == 2 {
			listUnordered = append(listUnordered, b[1])
		}
	}
	sort.Strings(listUnordered)
	fmt.Println(listUnordered)

	if len(listUnordered) >= 2 {
		return listUnordered[len(listUnordered)-1],
			listUnordered[len(listUnordered)-2],
			nil
	}
	return "", "", errors.New("Not enough partitions to run delta")
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

WHERE n.importdate='%s'
AND n2.importdate='%s'

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

const agencyNewSQL = `
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
LEFT JOIN abr_weekly_agency_data n2 on n.pid=n2.pid

WHERE n.importdate='%s'
AND n2.importdate='%s'

AND n2.pid is null
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

		logs <- "State: " + *r2.QueryExecution.Status.State
		time.Sleep(time.Second)

		if *r2.QueryExecution.Status.State != "RUNNING" {
			logs <- "Ended: " + *r2.QueryExecution.Status.State
			break
		}
	}
}
