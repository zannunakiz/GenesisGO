package store

import (
	"strings"
	"testing"
)

func TestSanitizeDetailRedactsSecrets(t *testing.T) {
	cases := []struct {
		name     string
		in       string
		mustHide string
	}{
		{`authorization header`, `authorization: Bearer gg_secret123`, `gg_secret123`},
		{`api key`, `api_key=gg_abc123`, `gg_abc123`},
		{`password`, `password=topsecret`, `topsecret`},
		{`token JSON`, `{"token":"gg_tok_value"}`, `gg_tok_value`},
		{`x-api-key`, `x-api-key: gg_xkey`, `gg_xkey`},
	}
	for _, c := range cases {
		got := SanitizeDetail(c.in)
		if strings.Contains(got, c.mustHide) {
			t.Errorf("%s: SanitizeDetail(%q) still contains secret: %q", c.name, c.in, got)
		}
		if !strings.Contains(got, "[REDACTED]") {
			t.Errorf("%s: expected [REDACTED] in %q", c.name, c.in)
		}
	}

	kept := SanitizeDetail(`scenario_key=duplicate-delivery run_id=run-1`)
	if !strings.Contains(kept, `duplicate-delivery`) || !strings.Contains(kept, `run-1`) {
		t.Errorf("safe values were redacted: %q", kept)
	}
	if strings.Contains(kept, "[REDACTED]") {
		t.Errorf("unexpected redaction: %q", kept)
	}
}
