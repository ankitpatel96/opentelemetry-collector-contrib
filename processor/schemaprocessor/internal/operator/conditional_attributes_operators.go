package operator

import (
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/schemaprocessor/internal/migrate"
)

type SpanConditionalAttributeOperator struct {
	Migrator migrate.ConditionalAttributeSet
}

func (o SpanConditionalAttributeOperator) IsMigrator()  {}

func (o SpanConditionalAttributeOperator) Do(ss migrate.StateSelector, span ptrace.Span) error {
	return o.Migrator.Do(ss, span.Attributes(), span.Name())
}