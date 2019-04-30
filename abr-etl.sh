#!/bin/bash

export USER_ENC="AQICAHjAjhV7d3YGxLXMWTRObCPHtjQT0joQ4ZkhoypbVJ9fIQHuJuUm8IBYOZ3242iXQRjXAAAAezB5BgkqhkiG9w0BBwagbDBqAgEAMGUGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMxAxT8oW24rAJNbtiAgEQgDjwifBrEL3vHSY3LF9bs1fQaEbHk/tOoAkbTWpdg03NKJGdsW628pdFhH7AwtWxKmNo+njLlIZ+5w=="
export PASS_ENC="AQICAHjAjhV7d3YGxLXMWTRObCPHtjQT0joQ4ZkhoypbVJ9fIQFefuYdq1x049a/iPESUlFKAAAAaDBmBgkqhkiG9w0BBwagWTBXAgEAMFIGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMyY2jWUZOVygGcstEAgEQgCVKSUZMYnfxdQem2CEpMOqKgs30fzgCMv4E3ZcYvffcY9Ze7lZH"
export AWS_REGION="ap-southeast-2"
export CW_GROUP="abr-etl"
export BASE_PATH="/home/ubuntu/abr-etl"
export S3_DATA_BUCKET="sbv-abr-etl"
#export SKIP_DOWNLOAD="skip"

cd /home/ubuntu/abr-etl && \
    /usr/local/go/bin/go run abr-etl.go