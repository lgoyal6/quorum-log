package engine

import "testing"

// The shipped build must never contain the planted fault. This test runs
// without build tags, which is how the suite is run.
func TestShippedBuildHasNoPlantedFault(t *testing.T) {
	if PlantedApplyBeforeQuorum {
		t.Fatal("this build has the planted apply-before-quorum fault; it must only exist under the quorumlog_planted_apply_before_quorum build tag")
	}
}
