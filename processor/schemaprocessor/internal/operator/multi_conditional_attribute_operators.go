package operator

import (
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/schemaprocessor/internal/migrate"
)

type SpanEventConditionalAttributeOperator struct {
	migrator migrate.MultiConditionalAttributeSet
}

var _ migrate.Migrator = SpanEventConditionalAttributeOperator{}

func NewSpanEventConditionalAttributeOperator(migrator migrate.MultiConditionalAttributeSet) *SpanEventConditionalAttributeOperator {
	return &SpanEventConditionalAttributeOperator{migrator: migrator}
}

func (o SpanEventConditionalAttributeOperator) IsMigrator() {}

func (o SpanEventConditionalAttributeOperator) Do(ss migrate.StateSelector, span ptrace.Span) error {
	for e := 0; e < span.Events().Len(); e++ {
		event := span.Events().At(e)
		if err := o.migrator.Do(ss, event.Attributes(),
			map[string]string{
				"event.name": event.Name(),
				"span.name":  span.Name(),
			}); err != nil {
			return err
		}
	}
	return nil
}