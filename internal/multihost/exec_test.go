package multihost

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestShellQuote(t *testing.T) {
	cases := map[string]string{
		"uname -a":            `'uname -a'`,
		"echo 'hi'":           `'echo '\''hi'\'''`,
		`curl -d '{"a":1}' x`: `'curl -d '\''{"a":1}'\'' x'`,
	}
	for in, want := range cases {
		if got := ShellQuote(in); got != want {
			t.Errorf("ShellQuote(%q) = %s, want %s", in, got, want)
		}
	}
}

// The ssh executor is never invoked in a dry run, so its command
// construction is what has to be verified directly.
func TestSSHExecRunArgs(t *testing.T) {
	e := NewSSHExec()
	h := Host{ID: 1, Name: "host-a", SSH: "user@host-a", Advertise: "10.0.0.11", RaftPort: 9101, ChaosPort: 9301, DataDir: "/var/tmp/quorum-log/n1"}
	got := e.RunArgs(h, `curl -s -X POST http://127.0.0.1:9301/chaos/isolate -d '{"peers":[2,3]}'`)
	want := []string{
		"-o", "BatchMode=yes",
		"-o", "ConnectTimeout=10",
		"-o", "StrictHostKeyChecking=accept-new",
		"user@host-a",
		"/bin/sh", "-c",
		`'curl -s -X POST http://127.0.0.1:9301/chaos/isolate -d '\''{"peers":[2,3]}'\'''`,
	}
	if len(got) != len(want) {
		t.Fatalf("RunArgs = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("RunArgs[%d] = %q, want %q (full: %q)", i, got[i], want[i], got)
		}
	}
}

func TestSSHExecCopyArgs(t *testing.T) {
	e := NewSSHExec()
	h := Host{Name: "host-b", SSH: "user@host-b", DataDir: "/var/tmp/quorum-log/n2"}
	got := e.CopyArgs(h, "/local/bin/quorumlogd", h.BinaryPath())
	want := []string{
		"-o", "BatchMode=yes",
		"-o", "ConnectTimeout=10",
		"-o", "StrictHostKeyChecking=accept-new",
		"/local/bin/quorumlogd",
		"user@host-b:/var/tmp/quorum-log/n2/quorumlogd",
	}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("CopyArgs = %q, want %q", got, want)
	}
}

func TestSSHExecRefusesHostWithoutTarget(t *testing.T) {
	e := NewSSHExec()
	if _, err := e.Run(context.Background(), Host{Name: "no-ssh"}, "true"); err == nil {
		t.Fatal("Run without an ssh target must fail before executing anything")
	}
	if err := e.Copy(context.Background(), Host{Name: "no-ssh"}, "/a", "/b"); err == nil {
		t.Fatal("Copy without an ssh target must fail before executing anything")
	}
}

func TestLocalExecRunAndCopy(t *testing.T) {
	e := &LocalExec{}
	h := Host{ID: 1, Name: "local", SSH: "user@should-never-be-used", DataDir: t.TempDir()}
	out, err := e.Run(context.Background(), h, "printf ok")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out != "ok" {
		t.Fatalf("Run output = %q, want %q", out, "ok")
	}
	if _, err := e.Run(context.Background(), h, "exit 7"); err == nil {
		t.Fatal("a failing command must return an error")
	}

	src := filepath.Join(t.TempDir(), "bin")
	if err := os.WriteFile(src, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(h.DataDir, "quorumlogd")
	if err := e.Copy(context.Background(), h, src, dst); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	info, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o100 == 0 {
		t.Fatalf("copied file mode = %v, want the executable bit preserved", info.Mode())
	}
}

func TestNewExecutorFollowsMode(t *testing.T) {
	multi := &Inventory{Mode: ModeMultiHost}
	ex, err := NewExecutor(multi)
	if err != nil {
		t.Fatal(err)
	}
	if ex.Kind() != "ssh" {
		t.Fatalf("multi-host executor = %q, want ssh", ex.Kind())
	}
	dry := &Inventory{Mode: ModeLocalDryRun}
	ex, err = NewExecutor(dry)
	if err != nil {
		t.Fatal(err)
	}
	if ex.Kind() != "local" {
		t.Fatalf("dry-run executor = %q, want local", ex.Kind())
	}
	if _, err := NewExecutor(&Inventory{Mode: "something-else"}); err == nil {
		t.Fatal("an unknown mode must not produce an executor")
	}
}
