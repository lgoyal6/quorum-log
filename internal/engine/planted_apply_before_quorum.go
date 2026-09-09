//go:build quorumlog_planted_apply_before_quorum

package engine

import "quorumlog/internal/sm"

// PlantedApplyBeforeQuorum reports whether this binary was built with the
// planted apply-before-quorum fault. It is true only under the
// quorumlog_planted_apply_before_quorum build tag, which exists so the
// harness can prove its checks reject a broken node.
const PlantedApplyBeforeQuorum = true

// plantedApplyBeforeQuorum is the planted fault: the leader applies a
// proposed entry to its state machine and answers the client immediately,
// before the entry is committed by a quorum. A leader that is then isolated
// keeps acknowledging writes that the rest of the cluster never commits, so
// those acknowledged writes disappear once the partition heals. The
// linearizability check and the acknowledged-write invariant must both
// notice. This code is never in a shipped build.
func (e *Engine) plantedApplyBeforeQuorum(op sm.Op) {
	res := e.sm.Apply(op)
	key := propKey(op.ClientID, op.ReqID)
	if cb, ok := e.propWaiters[key]; ok {
		delete(e.propWaiters, key)
		cb(res, nil)
	}
}
