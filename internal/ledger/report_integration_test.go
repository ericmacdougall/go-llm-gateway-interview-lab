package ledger

import (
	"example.com/llmgateway/internal/core"
	"testing"
)

func TestPGReportPreservesScopeAndPendingUsage(t *testing.T) {
	ctx, s := fixture(t, 100)
	a := attempt("report", 10)
	a.Principal.Department = "engineering"
	a.Principal.App = "assistant"
	if ok, err := s.Reserve(ctx, a); err != nil || !ok {
		t.Fatal(err)
	}
	if err := s.MarkDispatch(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkUncertain(ctx, a.ID, core.Evidence{Outcome: "timeout"}); err != nil {
		t.Fatal(err)
	}
	groups, err := s.Report(ctx, "client")
	if err != nil || len(groups) != 1 {
		t.Fatalf("%v %v", groups, err)
	}
	if g := groups[0]; g.Department != "engineering" || g.App != "assistant" || g.Held != 10 || g.Confirmed != 0 || g.Attempts != 1 {
		t.Fatalf("%+v", g)
	}
	if ok, err := s.Settle(ctx, a.ID, core.Evidence{Final: true, Actual: 7, Ref: "usage:test"}); err != nil || !ok {
		t.Fatal(err)
	}
	groups, err = s.Report(ctx, "client")
	if err != nil || len(groups) != 1 || groups[0].Held != 0 || groups[0].Confirmed != 7 || groups[0].State != "settled" {
		t.Fatalf("%v %v", groups, err)
	}
	groups, err = s.Report(ctx, "foreign")
	if err != nil || len(groups) != 0 {
		t.Fatalf("foreign scope %v %v", groups, err)
	}
}
