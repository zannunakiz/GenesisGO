package invariants

import (
	"context"
	"fmt"
	"sort"
)

// Rule keys (stable across runs; used as invariant keys in scenarios).
const (
	RuleNoDuplicateEffect        = "no_duplicate_effect"
	RuleNoNegativeInventory      = "no_negative_inventory"
	RuleIdempotencyKeyUnique     = "idempotency_key_unique"
	RuleAllowedOrderTransition   = "allowed_order_transition"
)

// noDuplicateEffectRule: one event must not create duplicate successful effects.
type noDuplicateEffectRule struct{}

func (r noDuplicateEffectRule) Key() string { return RuleNoDuplicateEffect }

func (r noDuplicateEffectRule) Evaluate(_ context.Context, ev Evidence) (*Result, error) {
	groups := make(map[string][]EffectRecord)
	for _, eff := range ev.Effects {
		key := eff.EffectType + "|" + eff.AggregateID
		groups[key] = append(groups[key], eff)
	}

	var dupIDs []string
	var offendingGroups []string
	for key, effects := range groups {
		if len(effects) > 1 {
			offendingGroups = append(offendingGroups, key)
			for _, eff := range effects {
				dupIDs = append(dupIDs, eff.ID)
			}
		}
	}
	sort.Strings(offendingGroups)
	sort.Strings(dupIDs)

	if len(dupIDs) == 0 {
		return &Result{
			InvariantKey: r.Key(),
			Rule:         "no_duplicate_effect",
			Status:       StatusPass,
			Expected:     "at most one effect per (effect_type, aggregate_id)",
			Actual:       "no duplicates found",
			Evidence:     nil,
		}, nil
	}

	return &Result{
		InvariantKey: r.Key(),
		Rule:         "no_duplicate_effect",
		Status:       StatusFail,
		Expected:     "at most one effect per (effect_type, aggregate_id)",
		Actual:       fmt.Sprintf("duplicate effects found for %d group(s)", len(offendingGroups)),
		Evidence:     dupIDs,
	}, nil
}

// noNegativeInventoryRule: inventory cannot become negative.
type noNegativeInventoryRule struct{}

func (r noNegativeInventoryRule) Key() string { return RuleNoNegativeInventory }

func (r noNegativeInventoryRule) Evaluate(_ context.Context, ev Evidence) (*Result, error) {
	byAgg := make(map[string][]EffectRecord)
	for _, eff := range ev.Effects {
		if eff.EffectType != "inventory" {
			continue
		}
		byAgg[eff.AggregateID] = append(byAgg[eff.AggregateID], eff)
	}

	var failingAggs []string
	var evidence []string
	for agg, effects := range byAgg {
		sort.Slice(effects, func(i, j int) bool {
			if effects[i].RecordedAt != effects[j].RecordedAt {
				return effects[i].RecordedAt.Before(effects[j].RecordedAt)
			}
			return effects[i].ID < effects[j].ID
		})
		var total int64
		for _, eff := range effects {
			evidence = append(evidence, eff.ID)
			if eff.AmountCents != nil {
				total += *eff.AmountCents
			}
		}
		if total < 0 {
			failingAggs = append(failingAggs, agg)
		}
	}
	sort.Strings(failingAggs)
	sort.Strings(evidence)

	if len(failingAggs) == 0 {
		return &Result{
			InvariantKey: r.Key(),
			Rule:         "no_negative_inventory",
			Status:       StatusPass,
			Expected:     "inventory balance >= 0 for every aggregate",
			Actual:       "no negative inventory balances",
			Evidence:     nil,
		}, nil
	}

	return &Result{
		InvariantKey: r.Key(),
		Rule:         "no_negative_inventory",
		Status:       StatusFail,
		Expected:     "inventory balance >= 0 for every aggregate",
		Actual:       fmt.Sprintf("negative inventory for aggregate(s): %v", failingAggs),
		Evidence:     evidence,
	}, nil
}
// idempotencyKeyUniqueRule: a repeated idempotency key must not create an extra effect.
type idempotencyKeyUniqueRule struct{}

func (r idempotencyKeyUniqueRule) Key() string { return RuleIdempotencyKeyUnique }

func (r idempotencyKeyUniqueRule) Evaluate(_ context.Context, ev Evidence) (*Result, error) {
	byKey := make(map[string][]EffectRecord)
	for _, eff := range ev.Effects {
		if eff.IdempotencyKey == nil || *eff.IdempotencyKey == "" {
			continue
		}
		byKey[*eff.IdempotencyKey] = append(byKey[*eff.IdempotencyKey], eff)
	}

	var dupKeys []string
	var evidence []string
	for key, effects := range byKey {
		if len(effects) > 1 {
			dupKeys = append(dupKeys, key)
			for _, eff := range effects {
				evidence = append(evidence, eff.ID)
			}
		}
	}
	sort.Strings(dupKeys)
	sort.Strings(evidence)

	if len(dupKeys) == 0 {
		return &Result{
			InvariantKey: r.Key(),
			Rule:         "idempotency_key_unique",
			Status:       StatusPass,
			Expected:     "each idempotency key maps to at most one effect",
			Actual:       "no repeated idempotency keys",
			Evidence:     nil,
		}, nil
	}

	return &Result{
		InvariantKey: r.Key(),
		Rule:         "idempotency_key_unique",
		Status:       StatusFail,
		Expected:     "each idempotency key maps to at most one effect",
		Actual:       fmt.Sprintf("idempotency key(s) reused for multiple effect(s): %v", dupKeys),
		Evidence:     evidence,
	}, nil
}

// allowedOrderTransitionRule: order transitions must follow allowed rules.
// Once a non-positive (fulfill/cancel) order effect appears, no further
// positive (create) effect is allowed for the same aggregate.
type allowedOrderTransitionRule struct{}

func (r allowedOrderTransitionRule) Key() string { return RuleAllowedOrderTransition }

func (r allowedOrderTransitionRule) Evaluate(_ context.Context, ev Evidence) (*Result, error) {
	byAgg := make(map[string][]EffectRecord)
	for _, eff := range ev.Effects {
		if eff.EffectType != "order" {
			continue
		}
		byAgg[eff.AggregateID] = append(byAgg[eff.AggregateID], eff)
	}

	var failingAggs []string
	var evidence []string
	for agg, effects := range byAgg {
		sort.Slice(effects, func(i, j int) bool {
			if effects[i].RecordedAt != effects[j].RecordedAt {
				return effects[i].RecordedAt.Before(effects[j].RecordedAt)
			}
			return effects[i].ID < effects[j].ID
		})

		seenNonPositive := false
		for _, eff := range effects {
			evidence = append(evidence, eff.ID)
			amount := int64(0)
			if eff.AmountCents != nil {
				amount = *eff.AmountCents
			}
			if amount <= 0 {
				seenNonPositive = true
			} else if seenNonPositive {
				failingAggs = append(failingAggs, agg)
				break
			}
		}
	}
	sort.Strings(failingAggs)
	sort.Strings(evidence)

	if len(failingAggs) == 0 {
		return &Result{
			InvariantKey: r.Key(),
			Rule:         "allowed_order_transition",
			Status:       StatusPass,
			Expected:     "order effects follow create-then-fulfill/cancel sequence",
			Actual:       "no invalid order transitions",
			Evidence:     nil,
		}, nil
	}

	return &Result{
		InvariantKey: r.Key(),
		Rule:         "allowed_order_transition",
		Status:       StatusFail,
		Expected:     "order effects follow create-then-fulfill/cancel sequence",
		Actual:       fmt.Sprintf("invalid order transition for aggregate(s): %v", failingAggs),
		Evidence:     evidence,
	}, nil
}

// DefaultRules returns the four built-in sample-domain invariant rules.
func DefaultRules() []Rule {
	return []Rule{
		noDuplicateEffectRule{},
		noNegativeInventoryRule{},
		idempotencyKeyUniqueRule{},
		allowedOrderTransitionRule{},
	}
}

