package main

import (
	"testing"
)

func TestRenameDelta(t *testing.T) {
	type args struct {
		newest string
	}
	tests := []struct {
		name string
		args args
	}{
		{"Rename", args{newest: "2019-04-29"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RenameDelta(tt.args.newest, "Agency_Data", "updated")
		})
	}
}

func TestRunAgendyDataDelta(t *testing.T) {
	type args struct {
		newest   string
		previous string
	}
	tests := []struct {
		name string
		args args
	}{
		//{"Test 1", args{"2019-04-08", "2019-04-01"}},
		{"Test 1", args{"2019-04-29", "2019-04-08"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunAgendyDataDelta(tt.args.newest, tt.args.previous)
		})
	}
}
