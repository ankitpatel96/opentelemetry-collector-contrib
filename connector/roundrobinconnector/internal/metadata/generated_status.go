// Code generated by mdatagen. DO NOT EDIT.

package metadata

import (
	"go.opentelemetry.io/collector/component"
)

var (
	Type = component.MustNewType("roundrobin")
)

const (
	TracesToTracesStability   = component.StabilityLevelBeta
	MetricsToMetricsStability = component.StabilityLevelBeta
	LogsToLogsStability       = component.StabilityLevelBeta
)