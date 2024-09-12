package changelist

import (
	"fmt"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/schemaprocessor/internal/alias"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/schemaprocessor/internal/migrate"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/schemaprocessor/internal/operator"
)

type ChangeList struct {
	Migrators []migrate.Migrator
}

func (c ChangeList) Do(ss migrate.StateSelector, signal any) error {
	for i := 0; i < len(c.Migrators); i++ {
		var migrator migrate.Migrator
		// todo(ankit) in go1.23 switch to reversed iterators for this
		if ss == migrate.StateSelectorApply {
			migrator = c.Migrators[i]
		} else {
			migrator = c.Migrators[len(c.Migrators) - 1 -i]
		}
		// switch between operator types - what do the operators act on?
		switch thisMigrator := migrator.(type) {
		// this one acts on both spans and span events!
		case operator.SpanOperator:
			if span, ok := signal.(ptrace.Span); ok {
				if err := thisMigrator.Do(ss, span); err != nil {
					return err
				}
			} else {
				return fmt.Errorf("SpanOperator %T can't act on %T", thisMigrator, signal)
			}
		case operator.MetricOperator:
			if metric, ok := signal.(pmetric.Metric); ok {
				if err := thisMigrator.Do(ss, metric); err != nil {
					return err
				}
			} else {
				return fmt.Errorf("MetricOperator %T can't act on %T", thisMigrator, signal)
			}
		// no log operator because the only log operation is an attribute changeset
		// this block accepts all signals, and resources.  Is used for logs, and the all section
		// todo(ankit) switch these to specific typed ones?
		case migrate.AttributeChangeSet:
			switch attributeSignal := signal.(type) {
			case alias.Attributed:
				if err := thisMigrator.Do(ss, attributeSignal.Attributes()); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unsupported signal type %T for AttributeChangeSet", attributeSignal)
			}
		default:
			return fmt.Errorf("unsupported migrator type %T", thisMigrator)
		}
	}
		return nil
}

func (c ChangeList) Apply(signal any) error {
	return c.Do(migrate.StateSelectorApply, signal)
}

func (c ChangeList) Rollback(signal any) error {
	return c.Do(migrate.StateSelectorRollback, signal)
}