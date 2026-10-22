package broker

import (
	"context"
	"testing"
)

func TestNewRejectsEmptyURL(t *testing.T) {
	_, err := New(Config{})
	if err == nil {
		t.Fatal("expected error for empty NATS URL")
	}
	if err.Error() != "broker NATS URL is required" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewRejectsInvalidSubjectPrefix(t *testing.T) {
	_, err := New(Config{URL: "nats://127.0.0.1:4222", SubjectPrefix: "bad*prefix"})
	if err == nil {
		t.Fatal("expected error for invalid subject prefix")
	}
}

func TestNewRejectsUnreachableBroker(t *testing.T) {
	// A refused connection surfaces broker-not-ready without hanging.
	_, err := New(Config{URL: "nats://127.0.0.1:1"})
	if err == nil {
		t.Fatal("expected error when the broker is not reachable")
	}
}

func TestValidateRunID(t *testing.T) {
	valid := []string{"run-1", "run_1", "run1", "abc"}
	for _, id := range valid {
		if err := validateRunID(id); err != nil {
			t.Fatalf("expected %q to be valid: %v", id, err)
		}
	}
	invalid := []string{"", "bad.run", "bad*run", "bad>run", "bad/run", "bad:run", "bad run", "bad\trun"}
	for _, id := range invalid {
		if err := validateRunID(id); err == nil {
			t.Fatalf("expected %q to be invalid", id)
		}
	}
	long := ""
	for i := 0; i < 129; i++ {
		long += "a"
	}
	if err := validateRunID(long); err == nil {
		t.Fatal("expected over-long run id to be invalid")
	}
}

func TestValidateSubjectPrefix(t *testing.T) {
	if err := validateSubjectPrefix("gg.run"); err != nil {
		t.Fatalf("expected valid prefix: %v", err)
	}
	invalid := []string{"", "bad*prefix", "bad>prefix", "bad:prefix", "bad/prefix", "bad prefix"}
	for _, p := range invalid {
		if err := validateSubjectPrefix(p); err == nil {
			t.Fatalf("expected prefix %q to be invalid", p)
		}
	}
}

func TestSubjectsAreRunScopedAndIsolated(t *testing.T) {
	b := &jetStreamBroker{prefix: "gg.run"}

	s1, err := b.subjectFor("run-a", KindEvents)
	if err != nil {
		t.Fatalf("subjectFor run-a: %v", err)
	}
	s2, err := b.subjectFor("run-b", KindEvents)
	if err != nil {
		t.Fatalf("subjectFor run-b: %v", err)
	}
	if s1 == s2 {
		t.Fatal("expected distinct run ids to produce distinct subjects")
	}
	if s1 != "gg.run.run-a.events" {
		t.Fatalf("unexpected events subject: %q", s1)
	}

	cmd, err := b.subjectFor("run-a", KindCommands)
	if err != nil {
		t.Fatalf("subjectFor commands: %v", err)
	}
	if cmd == s1 {
		t.Fatal("expected events and commands subjects to differ")
	}

	if _, err := b.subjectFor("", KindEvents); err == nil {
		t.Fatal("expected empty run id to be rejected")
	}
	if _, err := b.subjectFor("bad.run", KindEvents); err == nil {
		t.Fatal("expected dotted run id to be rejected")
	}
	if b.subjectWildcard() != "gg.run.*.*" {
		t.Fatalf("unexpected wildcard: %q", b.subjectWildcard())
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	b := &jetStreamBroker{}
	if err := b.Close(); err != nil {
		t.Fatalf("first close should succeed: %v", err)
	}
	if err := b.Close(); err != ErrAlreadyClosed {
		t.Fatalf("second close should report already closed, got %v", err)
	}
}

func TestReadyIsFalseBeforeStart(t *testing.T) {
	b := &jetStreamBroker{}
	if b.Ready() {
		t.Fatal("expected Ready to be false before start")
	}
}

func TestPublishAndSubscribeRejectNotStarted(t *testing.T) {
	b := &jetStreamBroker{}
	if err := b.Publish(context.Background(), "run-1", []byte("{}")); err != ErrNotStarted {
		t.Fatalf("expected ErrNotStarted for publish before start, got %v", err)
	}
	if err := b.Subscribe(context.Background(), "run-1", nil); err != ErrNotStarted {
		t.Fatalf("expected ErrNotStarted for subscribe before start, got %v", err)
	}
}
