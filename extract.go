package main

import (
	"encoding/base64"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kms"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
)

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

func hide(s string) string {
	return strings.Repeat("•", len(s))
}

func UnzipABRData() {
	unzip := exec.Command("/usr/bin/unzip", "-o", "VIC_ABR_Extract.zip")
	out, err := unzip.CombinedOutput()
	if err != nil {
		logs <- err.Error()
	}
	logs <- string(out)
}
