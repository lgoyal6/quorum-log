package multihost

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

// Executor runs a shell command on a host and copies files to it. There are
// two implementations: SSHExec, which reaches a separate machine, and
// LocalExec, which runs everything on this machine for the single-host dry
// run. Nothing else in the driver knows which one it is using, so the dry
// run exercises the same flow.
type Executor interface {
	// Kind is "ssh" or "local"; it is recorded in every artifact.
	Kind() string
	// Run executes script through /bin/sh on the host and returns its
	// combined output.
	Run(ctx context.Context, h Host, script string) (string, error)
	// Copy places a local file at remotePath on the host, preserving the
	// executable bit.
	Copy(ctx context.Context, h Host, localPath, remotePath string) error
}

// NewExecutor picks the executor the inventory mode demands. This is the
// only place the mapping is made: a local-dry-run inventory can never reach
// another machine, and a multi-host inventory always goes over ssh.
func NewExecutor(inv *Inventory) (Executor, error) {
	switch inv.Mode {
	case ModeMultiHost:
		return NewSSHExec(), nil
	case ModeLocalDryRun:
		return &LocalExec{}, nil
	default:
		return nil, fmt.Errorf("unknown inventory mode %q", inv.Mode)
	}
}

// SSHExec runs host commands with ssh and copies files with scp. It is used
// only in multi-host mode.
type SSHExec struct {
	SSH     string   // ssh binary
	SCP     string   // scp binary
	Options []string // options shared by both binaries
}

// NewSSHExec returns the default ssh executor: no password prompts, a bounded
// connect timeout, and no interactive host-key question.
func NewSSHExec() *SSHExec {
	return &SSHExec{
		SSH: "ssh",
		SCP: "scp",
		Options: []string{
			"-o", "BatchMode=yes",
			"-o", "ConnectTimeout=10",
			"-o", "StrictHostKeyChecking=accept-new",
		},
	}
}

func (e *SSHExec) Kind() string { return "ssh" }

// RunArgs builds the ssh argument vector for script. The script is passed as
// a single quoted argument to the remote shell, so quoting is explicit here
// rather than left to however ssh happens to join arguments.
func (e *SSHExec) RunArgs(h Host, script string) []string {
	args := append([]string{}, e.Options...)
	return append(args, h.SSH, "/bin/sh", "-c", ShellQuote(script))
}

// CopyArgs builds the scp argument vector for one file.
func (e *SSHExec) CopyArgs(h Host, localPath, remotePath string) []string {
	args := append([]string{}, e.Options...)
	return append(args, localPath, h.SSH+":"+remotePath)
}

func (e *SSHExec) Run(ctx context.Context, h Host, script string) (string, error) {
	if h.SSH == "" {
		return "", fmt.Errorf("host %s has no ssh target", h.Name)
	}
	cmd := exec.CommandContext(ctx, e.SSH, e.RunArgs(h, script)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("ssh %s: %w: %s", h.SSH, err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

func (e *SSHExec) Copy(ctx context.Context, h Host, localPath, remotePath string) error {
	if h.SSH == "" {
		return fmt.Errorf("host %s has no ssh target", h.Name)
	}
	cmd := exec.CommandContext(ctx, e.SCP, e.CopyArgs(h, localPath, remotePath)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("scp %s -> %s: %w: %s", localPath, h.SSH+":"+remotePath, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// LocalExec runs host commands on this machine. It is used only by the
// single-host dry run, and it ignores the ssh target entirely, so it cannot
// reach another machine even if one is configured.
type LocalExec struct{}

func (e *LocalExec) Kind() string { return "local" }

func (e *LocalExec) Run(ctx context.Context, h Host, script string) (string, error) {
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", script)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("local sh: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

func (e *LocalExec) Copy(ctx context.Context, h Host, localPath, remotePath string) error {
	if localPath == remotePath {
		return nil
	}
	src, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer src.Close()
	info, err := src.Stat()
	if err != nil {
		return err
	}
	dst, err := os.OpenFile(remotePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, info.Mode().Perm())
	if err != nil {
		return err
	}
	if _, err := io.Copy(dst, src); err != nil {
		dst.Close()
		return err
	}
	return dst.Close()
}

// ShellQuote wraps s in single quotes for /bin/sh, escaping embedded single
// quotes.
func ShellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
