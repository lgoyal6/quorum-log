package sim

import (
	"testing"
)

// TestSmoke boots a 3-node cluster, elects a leader, writes, and reads.
func TestSmoke(t *testing.T) {
	c, err := NewCluster(Config{Seed: 1, NumNodes: 3, Net: DefaultNet, Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	c.Steps(100)
	if c.Leader() == 0 {
		t.Fatal("no leader after 100 steps")
	}
	if err := c.PutSync("setup", 1, "hello", "world", 500); err != nil {
		t.Fatal(err)
	}
	v, ok, err := c.ReadSync(2, "hello", 500)
	if err != nil || !ok || v != "world" {
		t.Fatalf("read: %q %v %v", v, ok, err)
	}
	if err := c.WaitConverged(500); err != nil {
		t.Fatal(err)
	}
	if c.Err() != nil {
		t.Fatal(c.Err())
	}
}
