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
	logEvents, err := logsToCWLogs(ld)
	if err != nil {
		return err
	}

	// TODO(jbd): This will cause a lot of contention if user
	// doesn't fine tune the queue batching.
	e.seqTokenMu.Lock()
	defer e.seqTokenMu.Unlock()

	out, err := e.client.PutLogEvents(&cloudwatchlogs.PutLogEventsInput{
		LogGroupName:  aws.String(e.config.LogGroupName),
		LogStreamName: aws.String(e.config.LogStreamName),
		LogEvents:     logEvents,
		SequenceToken: aws.String(e.seqToken),
	})
	if err != nil {
		return err
	}
	e.seqToken = *out.NextSequenceToken
	return nil
}

func logsToCWLogs(ld pdata.Logs) ([]*cloudwatchlogs.InputLogEvent, error) {
	n := ld.LogRecordCount()
	if n == 0 {
		return []*cloudwatchlogs.InputLogEvent{}, nil
	}

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
				out = append(out, logToCWLog(rl.Resource(), log))
			}
		}
	}
	return out, nil
}

func logToCWLog(resource pdata.Resource, log pdata.LogRecord) *cloudwatchlogs.InputLogEvent {
	event := &cloudwatchlogs.InputLogEvent{}
	event.Timestamp = aws.Int64(int64(log.Timestamp()) / 1000)
	event.Message = aws.String("{hello: world}")
	return event
}
