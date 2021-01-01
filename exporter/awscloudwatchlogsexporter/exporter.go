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
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/pdata"
)

type exporter struct {
	config *Config

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

	// TODO(jbd): This will cause a lot of contention if user
	// doesn't fine tune the queue batching.
	e.seqTokenMu.Lock()
	defer e.seqTokenMu.Unlock()

	input := &cloudwatchlogs.PutLogEventsInput{
		LogGroupName:  aws.String(e.config.LogGroupName),
		LogStreamName: aws.String(e.config.LogStreamName),
		LogEvents:     logEvents,
		SequenceToken: aws.String(e.seqToken),
	}
	out, err := e.client.PutLogEvents(input)
	if err != nil {
		return err
	}
	if info := out.RejectedLogEventsInfo; info != nil {
		return fmt.Errorf("log event rejected")
	}
	e.seqToken = *out.NextSequenceToken
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
	body := map[string]string{}
	body["name"] = log.Name()
	body["body"] = "" // TODO(jbd): Fix.
	body["severity_number"] = fmt.Sprintf("%d", log.SeverityNumber())
	body["severity_text"] = log.SeverityText()
	body["dropped_attributes_count"] = fmt.Sprintf("%d", log.DroppedAttributesCount())
	// TODO(jbd): add attributes.

	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return &cloudwatchlogs.InputLogEvent{
		Timestamp: aws.Int64(int64(log.Timestamp()) / (1000 * 1000)), // milliseconds
		Message:   aws.String(string(bodyJSON)),
	}, nil
}
