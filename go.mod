module github.com/open-telemetry/opentelemetry-collector-contrib

// NOTE:
// This go.mod is NOT used to build any official binary.
// To see the builder manifests used for official binaries,
// check https://github.com/open-telemetry/opentelemetry-collector-releases
//
// For the OpenTelemetry Collector Contrib distribution specifically, see
// https://github.com/open-telemetry/opentelemetry-collector-releases/tree/main/distributions/otelcol-contrib

go 1.22.0

// Replace references to modules that are in this repository with their relateive paths
// so that we always build with current (latest) version of the source code.

// see https://github.com/google/gnostic/issues/262
replace github.com/googleapis/gnostic v0.5.6 => github.com/googleapis/gnostic v0.5.5

retract (
	v0.76.2
	v0.76.1
	v0.65.0
	v0.37.0 // Contains dependencies on v0.36.0 components, which should have been updated to v0.37.0.
)

// see https://github.com/distribution/distribution/issues/3590
exclude github.com/docker/distribution v2.8.0+incompatible

// see https://github.com/DataDog/agent-payload/issues/218
exclude github.com/DataDog/agent-payload/v5 v5.0.59

// openshift removed all tags from their repo, use the pseudoversion from the release-3.9 branch HEAD
replace github.com/openshift/api v3.9.0+incompatible => github.com/openshift/api v0.0.0-20180801171038-322a19404e37

replace github.com/DataDog/opentelemetry-mapping-go/pkg/otlp/metrics => /Users/ankit.patel/code/otel/opentelemetry-mapping-go/pkg/otlp/metrics

replace github.com/DataDog/opentelemetry-mapping-go/pkg/otlp/attributes => /Users/ankit.patel/code/otel/opentelemetry-mapping-go/pkg/otlp/attributes

replace github.com/DataDog/datadog-agent/pkg/trace => /Users/ankit.patel/dd/datadog-agent/pkg/trace

replace github.com/DataDog/datadog-agent/pkg/proto => /Users/ankit.patel/dd/datadog-agent/pkg/proto

replace github.com/DataDog/datadog-agent/pkg/serializer => /Users/ankit.patel/dd/datadog-agent/pkg/serializer

replace github.com/DataDog/datadog-agent/comp/forwarder/defaultforwarder => /Users/ankit.patel/dd/datadog-agent/comp/forwarder/defaultforwarder

replace github.com/DataDog/datadog-agent/comp/forwarder/orchestrator/orchestratorinterface => /Users/ankit.patel/dd/datadog-agent/comp/forwarder/orchestrator/orchestratorinterface

replace github.com/DataDog/datadog-agent/comp/otelcol/otlp/components/exporter/serializerexporter => /Users/ankit.patel/dd/datadog-agent/comp/otelcol/otlp/components/exporter/serializerexporter

replace github.com/DataDog/datadog-agent/comp/serializer/compression => /Users/ankit.patel/dd/datadog-agent/comp/serializer/compression
