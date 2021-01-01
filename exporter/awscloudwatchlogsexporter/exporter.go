// Copyright 2020, OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package awscloudwatchlogsexporter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
)

type exporter struct {
	config *Config
	logger *zap.Logger

	startOnce sync.Once
	client    *cloudwatchlogs.CloudWatchLogs // available after startOnce

	seqTokenMu sync.Mutex
	seqToken   string
}

func (e *exporter) Start(ctx context.Context, host component.Host) error {
	awsConfig := &aws.Config{}
	if e.config.Region != "" {
		awsConfig.Region = aws.String(e.config.Region)
	}
	if e.config.Endpoint != "" {
		awsConfig.Endpoint = aws.String(e.config.Endpoint)
	}

	var startErr error
	e.startOnce.Do(func() {
		sess, err := session.NewSession(awsConfig)
		if err != nil {
			startErr = err
			return
		}
		e.client = cloudwatchlogs.New(sess)

		e.logger.Info("Retrieving Cloud Watch sequence token")
		out, err := e.client.DescribeLogStreams(&cloudwatchlogs.DescribeLogStreamsInput{
			LogGroupName:        aws.String(e.config.LogGroupName),
			LogStreamNamePrefix: aws.String(e.config.LogStreamName),
		})
		if err != nil {
			startErr = err
			return
		}
		if len(out.LogStreams) == 0 {
			startErr = errors.New("cannot find log group and stream")
			return
		}
		stream := out.LogStreams[0]
		e.seqToken = *stream.UploadSequenceToken // no need to guard
	})
	return startErr
}

func (e *exporter) Shutdown(ctx context.Context) error {
	// TODO(jbd): Signal shutdown to flush the logs.
	return nil
}

func (e *exporter) ConsumeLogs(ctx context.Context, ld pdata.Logs) error {
	logEvents, dropped, err := logsToCWLogs(ld)
	if err != nil {
		return err
	}
	if len(logEvents) == 0 {
		if dropped > 0 {
			return fmt.Errorf("dropped %d log entries", dropped)
		}
		return nil
	}

	var seqToken string
	e.seqTokenMu.Lock()
	seqToken = e.seqToken
	e.seqTokenMu.Unlock()

	e.logger.Info("Putting log events", zap.Int("num_of_events", len(logEvents)))
	input := &cloudwatchlogs.PutLogEventsInput{
		LogGroupName:  aws.String(e.config.LogGroupName),
		LogStreamName: aws.String(e.config.LogStreamName),
		LogEvents:     logEvents,
		SequenceToken: aws.String(seqToken),
	}
	out, err := e.client.PutLogEvents(input)
	if err != nil {
		return err
	}
	if info := out.RejectedLogEventsInfo; info != nil {
		return fmt.Errorf("log event rejected")
	}
	e.logger.Info("Log events are successfully put")

	// TODO(jbd): Investigate how the concurrency model of exporters
	// impact the use of sequence tokens.
	e.seqTokenMu.Lock()
	e.seqToken = *out.NextSequenceToken
	e.seqTokenMu.Unlock()
	return nil
}

func logsToCWLogs(ld pdata.Logs) ([]*cloudwatchlogs.InputLogEvent, int, error) {
	n := ld.ResourceLogs().Len()
	if n == 0 {
		return []*cloudwatchlogs.InputLogEvent{}, 0, nil
	}

	var dropped int
	out := make([]*cloudwatchlogs.InputLogEvent, 0) // TODO(jbd): set a better capacity

	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		ills := rl.InstrumentationLibraryLogs()
		for j := 0; j < ills.Len(); j++ {
			ils := ills.At(j)
			logs := ils.Logs()
			for k := 0; k < logs.Len(); k++ {
				log := logs.At(k)
				event, err := logToCWLog(rl.Resource(), log)
				if err != nil {
					dropped++
				} else {
					out = append(out, event)
				}
			}
		}
	}
	return out, dropped, nil
}

func logToCWLog(resource pdata.Resource, log pdata.LogRecord) (*cloudwatchlogs.InputLogEvent, error) {
	// TODO(jbd): Benchmark and improve the allocations.
	body := map[string]interface{}{}
	body["name"] = log.Name()
	body["body"] = attrValue(log.Body())
	body["severity_number"] = log.SeverityNumber()
	body["severity_text"] = log.SeverityText()
	body["dropped_attributes_count"] = log.DroppedAttributesCount()
	body["flags"] = log.Flags()
	if traceID := log.TraceID(); traceID.IsValid() {
		body["trace_id"] = traceID.HexString()
	}
	if spanID := log.SpanID(); spanID.IsValid() {
		body["span_id"] = spanID.HexString()
	}
	log.Attributes().ForEach(func(k string, v pdata.AttributeValue) {
		body[k] = attrValue(v)
	})

	// Add resource attributes.
	resource.Attributes().ForEach(func(k string, v pdata.AttributeValue) {
		body["resource_"+k] = attrValue(v)
	})

	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return &cloudwatchlogs.InputLogEvent{
		Timestamp: aws.Int64(int64(log.Timestamp()) / (1000 * 1000)), // milliseconds
		Message:   aws.String(string(bodyJSON)),
	}, nil
}

func attrValue(value pdata.AttributeValue) interface{} {
	switch value.Type() {
	case pdata.AttributeValueINT:
		return value.IntVal()
	case pdata.AttributeValueBOOL:
		return value.BoolVal()
	case pdata.AttributeValueDOUBLE:
		return value.DoubleVal()
	case pdata.AttributeValueSTRING:
		return value.StringVal()
	case pdata.AttributeValueMAP:
		values := map[string]interface{}{}
		value.MapVal().ForEach(func(k string, v pdata.AttributeValue) {
			values[k] = attrValue(v)
		})
		return values
	case pdata.AttributeValueARRAY:
		arrayVal := value.ArrayVal()
		values := make([]interface{}, arrayVal.Len())
		for i := 0; i < arrayVal.Len(); i++ {
			values[i] = attrValue(arrayVal.At(i))
		}
		return values
	case pdata.AttributeValueNULL:
		return nil
	default:
		return nil
	}
}
