package sim

import (
	"fmt"
	"testing"

	"github.com/anishathalye/porcupine"
	"go.etcd.io/raft/v3/raftpb"

	"quorumlog/internal/check"
	"quorumlog/internal/sm"
)

// Seeds is the documented seed set: every scenario below runs across all of
// these and is fully deterministic for each.
var Seeds = []int64{1, 7, 42, 1337, 99991}

func forSeeds(t *testing.T, fn func(t *testing.T, seed int64)) {
	for _, s := range Seeds {
		t.Run(fmt.Sprintf("seed=%d", s), func(t *testing.T) { fn(t, s) })
	}
}

func mustCluster(t *testing.T, cfg Config) *Cluster {
	t.Helper()
	if cfg.Dir == "" {
		cfg.Dir = t.TempDir()
	}
	c, err := NewCluster(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func waitLeader(t *testing.T, c *Cluster, maxSteps int) uint64 {
	t.Helper()
	for i := 0; i < maxSteps; i++ {
		if l := c.Leader(); l != 0 {
			return l
		}
		c.Step()
	}
	t.Fatalf("no leader after %d steps", maxSteps)
	return 0
}

func runClients(t *testing.T, c *Cluster, clients []*Client, maxSteps int) {
	t.Helper()
	for i := 0; i < maxSteps; i++ {
		done := true
		for _, cl := range clients {
			if !cl.Done() {
				done = false
				break
			}
		}
		if done {
			return
		}
		c.Step()
		if c.Err() != nil {
			t.Fatal(c.Err())
		}
	}
	for _, cl := range clients {
		if !cl.Done() {
			t.Fatalf("client %s finished only %d/%d ops after %d steps", cl.name, cl.opsDone, cl.cfg.NumOps, maxSteps)
		}
	}
}

func checkLinearizable(t *testing.T, c *Cluster) {
	t.Helper()
	hist := c.Recorder.History()
	if res := check.Check(hist); res != porcupine.Ok {
		t.Fatalf("history of %d ops is NOT linearizable: %v", len(hist), res)
	}
}

func finish(t *testing.T, c *Cluster, maxSteps int) {
	t.Helper()
	c.Heal()
	if err := c.WaitConverged(maxSteps); err != nil {
		t.Fatal(err)
	}
	if c.Err() != nil {
		t.Fatal(c.Err())
	}
}

// --- Scenario: leader crash during proposals -------------------------------

func TestLeaderCrashDuringProposals(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c := mustCluster(t, Config{Seed: seed, NumNodes: 3, Net: DefaultNet})
		clients := []*Client{
			c.AddClient(ClientConfig{NumOps: 30, GetFrac: 0.25, DelFrac: 0.05}),
			c.AddClient(ClientConfig{NumOps: 30, GetFrac: 0.25, DelFrac: 0.05}),
			c.AddClient(ClientConfig{NumOps: 30, GetFrac: 0.25}),
		}
		lead := waitLeader(t, c, 500)
		// Let proposals flow, then crash the leader mid-stream.
		for i := 0; i < 5000; i++ {
			total := 0
			for _, cl := range clients {
				total += cl.DoneOps()
			}
			if total >= 10 {
				break
			}
			c.Step()
		}
		c.Crash(lead)
		runClients(t, c, clients, 20000)
		finish(t, c, 2000)
		checkLinearizable(t, c)
	})
}

// --- Scenario: minority partition ------------------------------------------

func testMinorityPartition(t *testing.T, numNodes int) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c := mustCluster(t, Config{Seed: seed, NumNodes: numNodes, Net: DefaultNet})
		clients := []*Client{
			c.AddClient(ClientConfig{NumOps: 25, GetFrac: 0.3}),
			c.AddClient(ClientConfig{NumOps: 25, GetFrac: 0.3}),
		}
		lead := waitLeader(t, c, 500)
		// Partition the leader (plus one follower on 5-node clusters) away.
		minority := []uint64{lead}
		if numNodes == 5 {
			for _, id := range c.AliveIDs() {
				if id != lead {
					minority = append(minority, id)
					break
				}
			}
		}
		var majority []uint64
		for _, id := range c.AliveIDs() {
			inMin := false
			for _, m := range minority {
				if m == id {
					inMin = true
				}
			}
			if !inMin {
				majority = append(majority, id)
			}
		}
		c.Partition(minority, majority)
		// The majority must elect a new leader and clients must make progress.
		c.Steps(200)
		newLead := c.Leader()
		if newLead == 0 {
			t.Fatal("majority did not elect a leader during partition")
		}
		if newLead == lead {
			t.Fatalf("old minority leader %d still counted as quorum leader", lead)
		}
		runClients(t, c, clients, 30000)
		finish(t, c, 3000)
		checkLinearizable(t, c)
	})
}

func TestMinorityPartition3(t *testing.T) { testMinorityPartition(t, 3) }
func TestMinorityPartition5(t *testing.T) { testMinorityPartition(t, 5) }

// --- Scenario: old leader rejoins ------------------------------------------

func TestOldLeaderRejoins(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c := mustCluster(t, Config{Seed: seed, NumNodes: 3, Net: DefaultNet})
		oldLead := waitLeader(t, c, 500)
		if err := c.PutSync("setup", 1, "stable", "before-partition", 1000); err != nil {
			t.Fatal(err)
		}
		c.Partition([]uint64{oldLead}, otherIDs(c, oldLead))

		// Propose to the isolated old leader: it still believes it leads, so
		// the proposal is accepted locally but can never commit.
		lostAcked := false
		err := c.Node(oldLead).Eng.Propose(
			sm.Op{Type: sm.OpPut, Key: "lost", Val: "minority-write", ClientID: "lost", ReqID: 1},
			func(sm.Result, error) { lostAcked = true },
		)
		if err != nil {
			t.Fatalf("old leader rejected proposal unexpectedly: %v", err)
		}

		// Majority elects a new leader and commits writes.
		c.Steps(200)
		newLead := c.Leader()
		if newLead == 0 || newLead == oldLead {
			t.Fatalf("no new majority leader (old=%d new=%d)", oldLead, newLead)
		}
		if err := c.PutSync("setup", 2, "stable", "after-partition", 2000); err != nil {
			t.Fatal(err)
		}
		if lostAcked {
			t.Fatal("write on isolated minority leader was acknowledged")
		}

		// Heal: old leader must step down, discard the uncommitted entry, and
		// converge on the majority's log.
		c.Heal()
		if err := c.WaitConverged(3000); err != nil {
			t.Fatal(err)
		}
		if c.Node(oldLead).Eng.IsLeader() && c.Leader() != oldLead {
			t.Fatal("old leader did not step down after rejoining")
		}
		if lostAcked {
			t.Fatal("uncommitted minority write acknowledged after heal")
		}
		if v, ok := c.Node(oldLead).Eng.SM().Get("lost"); ok {
			t.Fatalf("uncommitted minority write survived: %q", v)
		}
		if v, _ := c.Node(oldLead).Eng.SM().Get("stable"); v != "after-partition" {
			t.Fatalf("old leader state = %q, want after-partition", v)
		}
	})
}

func otherIDs(c *Cluster, except uint64) []uint64 {
	var out []uint64
	for _, id := range c.AliveIDs() {
		if id != except {
			out = append(out, id)
		}
	}
	return out
}

// --- Scenario: follower falls behind the retained log, snapshot install ----

func TestFollowerSnapshotCatchup(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c := mustCluster(t, Config{
			Seed: seed, NumNodes: 3, Net: DefaultNet,
			SnapshotThreshold: 10, SnapshotTrailing: 2,
		})
		lead := waitLeader(t, c, 500)
		var follower uint64
		for _, id := range c.AliveIDs() {
			if id != lead {
				follower = id
				break
			}
		}
		if err := c.PutSync("setup", 1, "k", "v1", 1000); err != nil {
			t.Fatal(err)
		}
		crashIndex := c.Node(follower).Eng.AppliedIndex()
		c.Crash(follower)

		// Push the log far past the snapshot threshold so the leader compacts
		// beyond the crashed follower's position.
		for i := 0; i < 40; i++ {
			if err := c.PutSync("setup", uint64(i+2), fmt.Sprintf("key%d", i%8), fmt.Sprintf("v%d", i), 2000); err != nil {
				t.Fatal(err)
			}
		}
		curLead := c.Leader()
		leadFirst, err := c.Node(curLead).Eng.Storage().FirstIndex()
		if err != nil {
			t.Fatal(err)
		}
		if leadFirst <= crashIndex {
			t.Fatalf("leader log not compacted past follower position (first=%d, follower=%d)", leadFirst, crashIndex)
		}

		// Restart the follower: it must catch up via snapshot install.
		if err := c.Restart(follower); err != nil {
			t.Fatal(err)
		}
		finish(t, c, 3000)
		f := c.Node(follower).Eng
		if f.SnapIndex() <= crashIndex {
			t.Fatalf("follower did not install a snapshot (snapIndex=%d, crashed at %d)", f.SnapIndex(), crashIndex)
		}
		if v, _ := f.SM().Get("key7"); v == "" {
			t.Fatal("follower state missing data after snapshot install")
		}
	})
}

// --- Scenario: membership change (add then remove) -------------------------

func TestMembershipChange(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c := mustCluster(t, Config{Seed: seed, NumNodes: 3, Net: DefaultNet})
		waitLeader(t, c, 500)
		if err := c.PutSync("setup", 1, "a", "1", 1000); err != nil {
			t.Fatal(err)
		}

		// Add node 4.
		if err := c.AddNodeProcess(4); err != nil {
			t.Fatal(err)
		}
		cc := raftpb.ConfChange{ID: 1, Type: raftpb.ConfChangeAddNode, NodeID: 4}
		if err := c.ProposeConfChangeSync(cc, 3000); err != nil {
			t.Fatal(err)
		}
		if err := c.WaitConverged(3000); err != nil {
			t.Fatalf("node 4 did not catch up: %v", err)
		}
		if v, _ := c.Node(4).Eng.SM().Get("a"); v != "1" {
			t.Fatalf("new node state = %q, want 1", v)
		}
		if got := len(c.Node(4).Eng.Voters()); got != 4 {
			t.Fatalf("voter count on new node = %d, want 4", got)
		}

		// Remove a non-leader original node.
		lead := c.Leader()
		var victim uint64
		for _, id := range []uint64{1, 2, 3} {
			if id != lead {
				victim = id
				break
			}
		}
		cc = raftpb.ConfChange{ID: 2, Type: raftpb.ConfChangeRemoveNode, NodeID: victim}
		if err := c.ProposeConfChangeSync(cc, 3000); err != nil {
			t.Fatal(err)
		}
		c.Steps(100) // let the removal propagate
		c.Crash(victim)

		// The remaining 3 voters must still commit.
		if err := c.PutSync("setup", 2, "b", "2", 2000); err != nil {
			t.Fatal(err)
		}
		finish(t, c, 3000)
		lead = c.Leader()
		voters := c.Node(lead).Eng.Voters()
		if len(voters) != 3 {
			t.Fatalf("voters after remove = %v, want 3 voters", voters)
		}
		for _, v := range voters {
			if v == victim {
				t.Fatalf("removed node %d still a voter: %v", victim, voters)
			}
		}
	})
}

// --- Scenario: full-cluster restart from persisted state --------------------

func TestRestartFromPersistedState(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c := mustCluster(t, Config{
			Seed: seed, NumNodes: 3, Net: DefaultNet,
			SnapshotThreshold: 10, SnapshotTrailing: 2,
		})
		waitLeader(t, c, 500)
		// Enough writes to force at least one snapshot plus post-snapshot log
		// entries, so restart exercises snapshot + WAL replay.
		for i := 0; i < 25; i++ {
			if err := c.PutSync("setup", uint64(i+1), fmt.Sprintf("key%d", i), fmt.Sprintf("v%d", i), 2000); err != nil {
				t.Fatal(err)
			}
		}
		lead := c.Leader()
		if c.Node(lead).Eng.SnapIndex() == 0 {
			t.Fatal("expected a snapshot before restart")
		}
		wantHash := c.Node(lead).Eng.SM().Hash()

		// Stop everything, then restart from disk.
		for _, id := range []uint64{1, 2, 3} {
			c.Crash(id)
		}
		for _, id := range []uint64{1, 2, 3} {
			if err := c.Restart(id); err != nil {
				t.Fatal(err)
			}
		}
		waitLeader(t, c, 1000)
		if err := c.WaitConverged(3000); err != nil {
			t.Fatal(err)
		}
		for _, id := range []uint64{1, 2, 3} {
			if got := c.Node(id).Eng.SM().Hash(); got != wantHash {
				t.Fatalf("node %d state hash %x != pre-restart %x", id, got, wantHash)
			}
		}
		// Every key must be readable (linearizable) after restart.
		for i := 0; i < 25; i++ {
			v, ok, err := c.ReadSync(c.Leader(), fmt.Sprintf("key%d", i), 2000)
			if err != nil || !ok || v != fmt.Sprintf("v%d", i) {
				t.Fatalf("key%d after restart: %q %v %v", i, v, ok, err)
			}
		}
		// And the cluster must still accept writes.
		if err := c.PutSync("setup", 100, "post", "restart", 2000); err != nil {
			t.Fatal(err)
		}
	})
}

// --- Scenario: duplicate client request applies once ------------------------

func TestDuplicateClientRequest(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c := mustCluster(t, Config{Seed: seed, NumNodes: 3, Net: DefaultNet})
		waitLeader(t, c, 500)
		// The same request (client "dup", req 1) committed twice: an append,
		// then a retried duplicate of the exact same request.
		if err := c.AppendSync("dup", 1, "d", "x", 2000); err != nil {
			t.Fatal(err)
		}
		if err := c.AppendSync("dup", 1, "d", "x", 2000); err != nil {
			t.Fatal(err)
		}
		finish(t, c, 2000)
		for _, id := range c.AliveIDs() {
			if v, _ := c.Node(id).Eng.SM().Get("d"); v != "x" {
				t.Fatalf("node %d: duplicate applied more than once: %q (want %q)", id, v, "x")
			}
		}
	})
}

// --- Scenario: concurrent reads and writes under a faulty network ----------

func testConcurrent(t *testing.T, numNodes int) {
	forSeeds(t, func(t *testing.T, seed int64) {
		c := mustCluster(t, Config{
			Seed: seed, NumNodes: numNodes,
			Net: NetConfig{DropProb: 0.05, DupProb: 0.05, MinDelay: 1, MaxDelay: 5},
		})
		var clients []*Client
		for i := 0; i < 4; i++ {
			clients = append(clients, c.AddClient(ClientConfig{NumOps: 25, GetFrac: 0.35, DelFrac: 0.05}))
		}
		waitLeader(t, c, 1000)
		runClients(t, c, clients, 40000)
		finish(t, c, 4000)
		checkLinearizable(t, c)
	})
}

func TestConcurrentReadsWrites3(t *testing.T) { testConcurrent(t, 3) }
func TestConcurrentReadsWrites5(t *testing.T) { testConcurrent(t, 5) }

// --- Determinism: identical seed, identical run -----------------------------

func TestDeterministicReplay(t *testing.T) {
	forSeeds(t, func(t *testing.T, seed int64) {
		run := func(dir string) (uint64, uint64, int) {
			c, err := NewCluster(Config{Seed: seed, NumNodes: 3,
				Net: NetConfig{DropProb: 0.1, DupProb: 0.1, MinDelay: 1, MaxDelay: 4}, Dir: dir})
			if err != nil {
				t.Fatal(err)
			}
			cl := c.AddClient(ClientConfig{NumOps: 20, GetFrac: 0.3})
			for i := 0; i < 20000 && !cl.Done(); i++ {
				c.Step()
			}
			c.Heal()
			if err := c.WaitConverged(3000); err != nil {
				t.Fatal(err)
			}
			lead := c.Leader()
			return c.TraceHash(), c.Node(lead).Eng.SM().Hash(), len(c.Recorder.History())
		}
		t1, h1, n1 := run(t.TempDir())
		t2, h2, n2 := run(t.TempDir())
		if t1 != t2 || h1 != h2 || n1 != n2 {
			t.Fatalf("same seed diverged: trace %x/%x state %x/%x hist %d/%d", t1, t2, h1, h2, n1, n2)
		}
	})
}
