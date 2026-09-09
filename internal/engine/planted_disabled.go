//go:build !quorumlog_planted_apply_before_quorum

package engine

import "quorumlog/internal/sm"

// PlantedApplyBeforeQuorum reports whether this binary was built with the
// planted apply-before-quorum fault. It is false in every shipped build.
const PlantedApplyBeforeQuorum = false

// plantedApplyBeforeQuorum does nothing in the shipped build. The planted
// version lives in planted_apply_before_quorum.go behind the
// quorumlog_planted_apply_before_quorum build tag and exists only so the
// test harness can prove its checks can fail.
func (e *Engine) plantedApplyBeforeQuorum(op sm.Op) {}
