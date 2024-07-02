// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package datadogexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter"

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/DataDog/datadog-agent/comp/otelcol/logsagentpipeline"
	"github.com/DataDog/datadog-agent/comp/otelcol/logsagentpipeline/logsagentpipelineimpl"
	"github.com/DataDog/datadog-agent/comp/otelcol/otlp/components/exporter/logsagentexporter"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/DataDog/opentelemetry-mapping-go/pkg/inframetadata"
	"github.com/DataDog/opentelemetry-mapping-go/pkg/otlp/attributes"
	"github.com/DataDog/opentelemetry-mapping-go/pkg/otlp/attributes/source"
	logsmapping "github.com/DataDog/opentelemetry-mapping-go/pkg/otlp/logs"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterbatcher"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/internal/clientutil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/internal/hostmetadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/internal/logs"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/internal/scrub"
)

const (
	// logSourceName specifies the Datadog source tag value to be added to logs sent from the Datadog exporter.
	logSourceName = "otlp_log_ingestion"
	// otelSource specifies a source to be added to all logs sent from the Datadog exporter. The tag has key `otel_source` and the value specified on this constant.
	otelSource = "datadog_exporter"
)

type logsRequest struct {
	Sender *logs.Sender
	Ld     *[]datadogV2.HTTPLogItem
}

func mergeLogs(_ context.Context, r1 exporterhelper.Request, r2 exporterhelper.Request) (exporterhelper.Request, error) {
	lr1, ok1 := r1.(*logsRequest)
	lr2, ok2 := r2.(*logsRequest)
	if !ok1 || !ok2 {
		return nil, errors.New("invalid input type")
	}
	*lr1.Ld = append(*lr1.Ld, *lr2.Ld...)
	lr2.Ld = nil
	return lr1, nil
}

func mergeSplitLogs(_ context.Context, cfg exporterbatcher.MaxSizeConfig, r1, r2 exporterhelper.Request) ([]exporterhelper.Request, error) {
	var (
		res          []exporterhelper.Request
		destReq      *logsRequest
		capacityLeft = cfg.MaxSizeItems
	)
	// iterate through the input requests (optional)
	for _, req := range []exporterhelper.Request{r1, r2} {
		if req == nil {
			continue
		}
		srcReq, ok := req.(*logsRequest)
		if !ok {
			return nil, errors.New("invalid input type")
		}
		if len(*srcReq.Ld) <= capacityLeft {
			if destReq == nil {
				destReq = srcReq
			} else {
				*destReq.Ld = append(*srcReq.Ld, *destReq.Ld...)
				srcReq.Ld = nil
			}
			capacityLeft -= len(*destReq.Ld)
			continue
		}

		for {
			if len(*srcReq.Ld) == 0 {
				break
			}
			extractCount := min(len(*srcReq.Ld), capacityLeft)
			extractedLogs := (*srcReq.Ld)[:extractCount]
			*srcReq.Ld = (*srcReq.Ld)[extractCount:]
			capacityLeft = capacityLeft - extractCount
			if destReq == nil {
				destReq = &logsRequest{Ld: &extractedLogs, Sender: srcReq.Sender}
			} else {
				*destReq.Ld = append(*destReq.Ld, extractedLogs...)
			}
			// Create new batch once capacity is reached.
			if capacityLeft == 0 {
				res = append(res, destReq)
				destReq = nil
				capacityLeft = cfg.MaxSizeItems
			}
		}
	}

	if destReq != nil {
		res = append(res, destReq)
	}
	return res, nil
}

func (l logsRequest) ItemsCount() int {
	return len(*l.Ld)
}

func (l logsRequest) Export(ctx context.Context) error {
	return l.Sender.SubmitLogs(ctx, *l.Ld)
}

func (exp *logsExporter) NewLogsRequest(ctx context.Context, ld plog.Logs) (exporterhelper.Request, error) {
	var err error
	defer func() { err = exp.scrubber.Scrub(err) }()
	logz := exp.translator.MapLogs(ctx, ld)
	lr := logsRequest{
		Sender: exp.sender,
		Ld:     &logz,
	}
	if exp.cfg.HostMetadata.Enabled {
		// start host metadata with resource attributes from
		// the first payload.
		exp.onceMetadata.Do(func() {
			attrs := pcommon.NewMap()
			if ld.ResourceLogs().Len() > 0 {
				attrs = ld.ResourceLogs().At(0).Resource().Attributes()
			}
			go hostmetadata.RunPusher(exp.ctx, exp.params, newMetadataConfigfromConfig(exp.cfg), exp.sourceProvider, attrs, exp.metadataReporter)
		})

		// Consume resources for host metadata
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			res := ld.ResourceLogs().At(i).Resource()
			consumeResource(exp.metadataReporter, res, exp.params.Logger)
		}
	}
	return lr, err
}

// just used in the classic logs exporter
type logsExporter struct {
	params           exporter.Settings
	cfg              *Config
	ctx              context.Context // ctx triggers shutdown upon cancellation
	scrubber         scrub.Scrubber  // scrubber scrubs sensitive information from error messages
	translator       *logsmapping.Translator
	sender           *logs.Sender
	onceMetadata     *sync.Once
	sourceProvider   source.Provider
	metadataReporter *inframetadata.Reporter
}

// newLogsExporter creates a new instance of logsExporter
func newLogsExporter(
	ctx context.Context,
	params exporter.Settings,
	cfg *Config,
	onceMetadata *sync.Once,
	attributesTranslator *attributes.Translator,
	sourceProvider source.Provider,
	metadataReporter *inframetadata.Reporter,
) (*logsExporter, error) {
	// create Datadog client
	// validation endpoint is provided by Metrics
	errchan := make(chan error)
	if isMetricExportV2Enabled() {
		apiClient := clientutil.CreateAPIClient(
			params.BuildInfo,
			cfg.Metrics.TCPAddrConfig.Endpoint,
			cfg.ClientConfig)
		go func() { errchan <- clientutil.ValidateAPIKey(ctx, string(cfg.API.Key), params.Logger, apiClient) }()
	} else {
		client := clientutil.CreateZorkianClient(string(cfg.API.Key), cfg.Metrics.TCPAddrConfig.Endpoint)
		go func() { errchan <- clientutil.ValidateAPIKeyZorkian(params.Logger, client) }()
	}
	// validate the apiKey
	if cfg.API.FailOnInvalidKey {
		if err := <-errchan; err != nil {
			return nil, err
		}
	}

	translator, err := logsmapping.NewTranslator(params.TelemetrySettings, attributesTranslator, otelSource)
	if err != nil {
		return nil, fmt.Errorf("failed to create logs translator: %w", err)
	}
	s := logs.NewSender(cfg.Logs.TCPAddrConfig.Endpoint, params.Logger, cfg.ClientConfig, cfg.Logs.DumpPayloads, string(cfg.API.Key))

	return &logsExporter{
		params:           params,
		cfg:              cfg,
		ctx:              ctx,
		translator:       translator,
		sender:           s,
		onceMetadata:     onceMetadata,
		scrubber:         scrub.NewScrubber(),
		sourceProvider:   sourceProvider,
		metadataReporter: metadataReporter,
	}, nil
}

var _ consumer.ConsumeLogsFunc = (*logsExporter)(nil).consumeLogs

// consumeLogs is implementation of consumer.ConsumeLogsFunc
func (exp *logsExporter) consumeLogs(ctx context.Context, ld plog.Logs) (err error) {
	defer func() { err = exp.scrubber.Scrub(err) }()
	if exp.cfg.HostMetadata.Enabled {
		// start host metadata with resource attributes from
		// the first payload.
		exp.onceMetadata.Do(func() {
			attrs := pcommon.NewMap()
			if ld.ResourceLogs().Len() > 0 {
				attrs = ld.ResourceLogs().At(0).Resource().Attributes()
			}
			go hostmetadata.RunPusher(exp.ctx, exp.params, newMetadataConfigfromConfig(exp.cfg), exp.sourceProvider, attrs, exp.metadataReporter)
		})

		// Consume resources for host metadata
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			res := ld.ResourceLogs().At(i).Resource()
			consumeResource(exp.metadataReporter, res, exp.params.Logger)
		}
	}

	payloads := exp.translator.MapLogs(ctx, ld)
	return exp.sender.SubmitLogs(exp.ctx, payloads)
}

// newLogsAgentExporter creates new instances of the logs agent and the logs agent exporter
func newLogsAgentExporter(
	ctx context.Context,
	params exporter.Settings,
	cfg *Config,
	sourceProvider source.Provider,
) (logsagentpipeline.LogsAgent, exporter.Logs, error) {
	logComponent := newLogComponent(params.TelemetrySettings)
	cfgComponent := newConfigComponent(params.TelemetrySettings, cfg)
	logsAgentConfig := &logsagentexporter.Config{
		OtelSource:    otelSource,
		LogSourceName: logSourceName,
	}
	hostnameComponent := logs.NewHostnameService(sourceProvider)
	logsAgent := logsagentpipelineimpl.NewLogsAgent(logsagentpipelineimpl.Dependencies{
		Log:      logComponent,
		Config:   cfgComponent,
		Hostname: hostnameComponent,
	})
	err := logsAgent.Start(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create logs agent: %w", err)
	}
	pipelineChan := logsAgent.GetPipelineProvider().NextPipelineChan()
	logsAgentExporter, err := logsagentexporter.NewFactory(pipelineChan).CreateLogsExporter(ctx, params, logsAgentConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create logs agent exporter: %w", err)
	}
	return logsAgent, logsAgentExporter, nil
}
