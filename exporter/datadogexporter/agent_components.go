// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package datadogexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter"

import (
	"context"
	"fmt"
	"strings"

	coreconfig "github.com/DataDog/datadog-agent/comp/core/config"
	"github.com/DataDog/datadog-agent/comp/core/log"
	"github.com/DataDog/datadog-agent/comp/forwarder/defaultforwarder"
	"github.com/DataDog/datadog-agent/comp/forwarder/orchestrator/orchestratorinterface"
	"github.com/DataDog/datadog-agent/comp/otelcol/otlp/components/exporter/serializerexporter"
	"github.com/DataDog/datadog-agent/comp/serializer/compression"
	"github.com/DataDog/datadog-agent/comp/serializer/compression/compressionimpl/strategy"
	pkgconfigmodel "github.com/DataDog/datadog-agent/pkg/config/model"
	pkgconfigsetup "github.com/DataDog/datadog-agent/pkg/config/setup"
	"github.com/DataDog/datadog-agent/pkg/serializer"
	"github.com/DataDog/opentelemetry-mapping-go/pkg/otlp/attributes"
	otlpmetrics "github.com/DataDog/opentelemetry-mapping-go/pkg/otlp/metrics"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"go.uber.org/zap"
)

func newLogComponent(set component.TelemetrySettings) log.Component {
	zlog := &zaplogger{
		logger: set.Logger,
	}
	return zlog
}

func newConfigComponent(set component.TelemetrySettings, cfg *Config) coreconfig.Component {
	pkgconfig := pkgconfigmodel.NewConfig("DD", "DD", strings.NewReplacer(".", "_"))

	// Set the API Key
	pkgconfig.Set("api_key", string(cfg.API.Key), pkgconfigmodel.SourceFile)
	pkgconfig.Set("site", cfg.API.Site, pkgconfigmodel.SourceFile)
	pkgconfig.Set("logs_enabled", true, pkgconfigmodel.SourceDefault)
	pkgconfig.Set("log_level", set.Logger.Level().String(), pkgconfigmodel.SourceFile)
	pkgconfig.Set("logs_config.batch_wait", cfg.Logs.BatchWait, pkgconfigmodel.SourceFile)
	pkgconfig.Set("logs_config.use_compression", cfg.Logs.UseCompression, pkgconfigmodel.SourceFile)
	pkgconfig.Set("logs_config.compression_level", cfg.Logs.CompressionLevel, pkgconfigmodel.SourceFile)
	pkgconfig.Set("logs_config.logs_dd_url", cfg.Logs.TCPAddrConfig.Endpoint, pkgconfigmodel.SourceFile)
	pkgconfig.Set("logs_config.auditor_ttl", pkgconfigsetup.DefaultAuditorTTL, pkgconfigmodel.SourceDefault)
	pkgconfig.Set("logs_config.batch_max_content_size", pkgconfigsetup.DefaultBatchMaxContentSize, pkgconfigmodel.SourceDefault)
	pkgconfig.Set("logs_config.batch_max_size", pkgconfigsetup.DefaultBatchMaxSize, pkgconfigmodel.SourceDefault)
	pkgconfig.Set("logs_config.force_use_http", true, pkgconfigmodel.SourceDefault)
	pkgconfig.Set("logs_config.input_chan_size", pkgconfigsetup.DefaultInputChanSize, pkgconfigmodel.SourceDefault)
	pkgconfig.Set("logs_config.max_message_size_bytes", pkgconfigsetup.DefaultMaxMessageSizeBytes, pkgconfigmodel.SourceDefault)
	pkgconfig.Set("logs_config.force_use_http", true, pkgconfigmodel.SourceDefault)
	pkgconfig.Set("logs_config.run_path", "/opt/datadog-agent/run", pkgconfigmodel.SourceDefault)
	pkgconfig.Set("logs_config.sender_backoff_factor", pkgconfigsetup.DefaultLogsSenderBackoffFactor, pkgconfigmodel.SourceDefault)
	pkgconfig.Set("logs_config.sender_backoff_base", pkgconfigsetup.DefaultLogsSenderBackoffBase, pkgconfigmodel.SourceDefault)
	pkgconfig.Set("logs_config.sender_backoff_max", pkgconfigsetup.DefaultLogsSenderBackoffMax, pkgconfigmodel.SourceDefault)
	pkgconfig.Set("logs_config.sender_recovery_interval", pkgconfigsetup.DefaultForwarderRecoveryInterval, pkgconfigmodel.SourceDefault)
	pkgconfig.Set("logs_config.stop_grace_period", 30, pkgconfigmodel.SourceDefault)
	pkgconfig.Set("logs_config.use_v2_api", true, pkgconfigmodel.SourceDefault)
	pkgconfig.SetKnown("logs_config.dev_mode_no_ssl")

	return pkgconfig
}

func newSerializer(set component.TelemetrySettings, cfg *Config) (*serializer.Serializer, error) {
	var f defaultforwarder.Component
	var c coreconfig.Component
	var s *serializer.Serializer
	app := fx.New(
		fx.WithLogger(func(log *zap.Logger) fxevent.Logger {
			return &fxevent.ZapLogger{Logger: log}
		}),
		fx.Supply(set.Logger),
		fx.Supply(set),
		fx.Supply(cfg),
		fx.Provide(newLogComponent),
		fx.Provide(newConfigComponent),

		fx.Provide(func(c coreconfig.Component, l log.Component) (defaultforwarder.Params, error) {
			return defaultforwarder.NewParams(), nil
		}),
		fx.Provide(func(c defaultforwarder.Component) (defaultforwarder.Forwarder, error) {
			return defaultforwarder.Forwarder(c), nil
		}),
		fx.Provide(func() string {
			return ""
		}),
		fx.Provide(NewOrchestratorinterfaceimpl),
		fx.Provide(serializer.NewSerializer),
		fx.Provide(strategy.NewZlibStrategy),
		fx.Provide(func(s *strategy.ZlibStrategy) compression.Component {
			return s
		}),
		defaultforwarder.Module(defaultforwarder.NewParams()),
		fx.Populate(&f),
		fx.Populate(&c),
		fx.Populate(&s),
	)
	fmt.Printf("### done with app\n")
	if err := app.Err(); err != nil {
		return nil, err
	}
	go func() {
		forwarder := f.(*defaultforwarder.DefaultForwarder)
		err := forwarder.Start()
		if err != nil {
			fmt.Printf("### error starting forwarder: %s\n", err)
		}
	}()
	return s, nil
}

type orchestratorinterfaceimpl struct {
	f defaultforwarder.Forwarder
}

func NewOrchestratorinterfaceimpl(f defaultforwarder.Forwarder) orchestratorinterface.Component {
	return &orchestratorinterfaceimpl{
		f: f,
	}
}

func (o *orchestratorinterfaceimpl) Get() (defaultforwarder.Forwarder, bool) {
	return o.f, true
}

func (o *orchestratorinterfaceimpl) Reset() {
	o.f = nil
}

type tagEnricher struct{}

func (t *tagEnricher) SetCardinality(cardinality string) error {
	return nil
}

func (t *tagEnricher) Enrich(ctx context.Context, extraTags []string, dimensions *otlpmetrics.Dimensions) []string {
	return nil
}

func newSerializerExporter(ctx context.Context, set exporter.Settings, cfg *Config) (*serializerexporter.Exporter, error) {
	s, err := newSerializer(set.TelemetrySettings, cfg)
	if err != nil {
		return nil, err
	}
	hostGetter := func(_ context.Context) (string, error) {
		return "", nil
	}
	exporterConfig := &serializerexporter.ExporterConfig{
		Metrics: serializerexporter.MetricsConfig{
			Enabled:  true,
			DeltaTTL: cfg.Metrics.DeltaTTL,
			ExporterConfig: serializerexporter.MetricsExporterConfig{
				ResourceAttributesAsTags:           cfg.Metrics.ExporterConfig.ResourceAttributesAsTags,
				InstrumentationScopeMetadataAsTags: cfg.Metrics.ExporterConfig.InstrumentationScopeMetadataAsTags,
			},
			HistConfig: serializerexporter.HistogramConfig{
				Mode: serializerexporter.HistogramMode(cfg.Metrics.HistConfig.Mode),
			},
		},
	}
	exporterConfig.QueueConfig = cfg.QueueSettings
	exporterConfig.TimeoutConfig = exporterhelper.TimeoutConfig{
		Timeout: cfg.Timeout,
	}

	// TODO: Ideally the attributes translator would be created once and reused
	// across all signals. This would need unifying the logsagent and serializer
	// exporters into a single exporter.
	attributesTranslator, err := attributes.NewTranslator(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	fmt.Printf("### created attributes translator\n")
	newExp, err := serializerexporter.NewExporter(set.TelemetrySettings, attributesTranslator, s, exporterConfig, &tagEnricher{}, hostGetter, nil)
	if err != nil {
		return nil, err
	}

	return newExp, nil
}