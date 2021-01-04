package awscloudwatchlogsexporter

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"go.opentelemetry.io/collector/consumer/pdata"
)

func TestLogToCWLog(t *testing.T) {
	tests := []struct {
		name     string
		resource pdata.Resource
		log      pdata.LogRecord
		want     *cloudwatchlogs.InputLogEvent
		wantErr  bool
	}{
		{
			name:     "basic",
			resource: testResource(),
			log:      testLogRecord(),
			want: &cloudwatchlogs.InputLogEvent{
				Timestamp: aws.Int64(1609719139),
				Message:   aws.String(`{"body":"hello world","dropped_attributes_count":4,"flags":255,"key1":1,"key2":"attr2","name":"test","resource_host":"abc123","resource_node":5,"severity_number":5,"severity_text":"debug","span_id":"0102030405060708","trace_id":"0102030405060708090a0b0c0d0e0f10"}`),
			},
		},
		{
			name:     "no resource",
			resource: pdata.NewResource(),
			log:      testLogRecord(),
			want: &cloudwatchlogs.InputLogEvent{
				Timestamp: aws.Int64(1609719139),
				Message:   aws.String(`{"body":"hello world","dropped_attributes_count":4,"flags":255,"key1":1,"key2":"attr2","name":"test","severity_number":5,"severity_text":"debug","span_id":"0102030405060708","trace_id":"0102030405060708090a0b0c0d0e0f10"}`),
			},
		},
		{
			name:     "no trace",
			resource: testResource(),
			log:      testLogRecordWithoutTrace(),
			want: &cloudwatchlogs.InputLogEvent{
				Timestamp: aws.Int64(1609719139),
				Message:   aws.String(`{"body":"hello world","dropped_attributes_count":4,"flags":0,"key1":1,"key2":"attr2","name":"test","resource_host":"abc123","resource_node":5,"severity_number":5,"severity_text":"debug"}`),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := logToCWLog(tt.resource, tt.log)
			if (err != nil) != tt.wantErr {
				t.Errorf("logToCWLog() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("logToCWLog() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func testResource() pdata.Resource {
	resource := pdata.NewResource()
	resource.Attributes().InsertString("host", "abc123")
	resource.Attributes().InsertInt("node", 5)
	return resource
}

func testLogRecord() pdata.LogRecord {
	record := pdata.NewLogRecord()
	record.SetName("test")
	record.SetSeverityNumber(5)
	record.SetSeverityText("debug")
	record.SetDroppedAttributesCount(4)
	record.Body().SetStringVal("hello world")
	record.Attributes().InsertInt("key1", 1)
	record.Attributes().InsertString("key2", "attr2")
	record.SetTraceID(pdata.NewTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	record.SetSpanID(pdata.NewSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	record.SetFlags(255)
	record.SetTimestamp(1609719139000000)
	return record
}

func testLogRecordWithoutTrace() pdata.LogRecord {
	record := pdata.NewLogRecord()
	record.SetName("test")
	record.SetSeverityNumber(5)
	record.SetSeverityText("debug")
	record.SetDroppedAttributesCount(4)
	record.Body().SetStringVal("hello world")
	record.Attributes().InsertInt("key1", 1)
	record.Attributes().InsertString("key2", "attr2")
	record.SetTimestamp(1609719139000000)
	return record
}
