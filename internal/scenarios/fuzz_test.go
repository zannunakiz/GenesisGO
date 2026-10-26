package scenarios

import (
	"encoding/json"
	"testing"
)

// FuzzValidate exercises scenario validation with arbitrary inputs to catch
// panics or unexpected acceptance of malformed scenarios. No test data
// contains secrets.
func FuzzValidate(f *testing.F) {
	valid := Scenario{
		Key:     "ok_key",
		Version: 1,
		Name:    "x",
		Steps:   []Step{{ID: "s1", Type: "publish", EventID: "e1"}},
	}
	validJSON, _ := json.Marshal(valid)
	f.Add(string(validJSON))
	f.Add(`{}`)
	f.Add(`not-json`)
	f.Add(`{"key":"Bad Key!","version":1,"name":"x","steps":[{"id":"s1","type":"publish","event_id":"e1"}]}`)
	f.Add(`{"key":"ok_key","version":0,"name":"x","steps":[{"id":"s1","type":"publish","event_id":"e1"}]}`)
	f.Add(`{"key":"ok_key","version":1,"name":"x","steps":[{"id":"s1","type":"exec_shell","event_id":"e1"}]}`)
	f.Add(`{"key":"ok_key","version":1,"name":"x","steps":[{"id":"s1","type":"publish"},{"id":"s1","type":"deliver","event_id":"e1"}]}`)
	f.Add(`{"key":"ok_key","version":1,"name":"x","steps":[{"id":"s1","type":"publish","event_id":"e1"}],"parameters":{"max_events":99999,"max_retries":3,"timeout_ms":15000,"concurrency":1}}`)

	f.Fuzz(func(t *testing.T, raw string) {
		var s Scenario
		if err := json.Unmarshal([]byte(raw), &s); err != nil {
			return
		}
		if err := Validate(s); err != nil {
			return
		}
		if !validKey(s.Key) {
			t.Fatal("validation passed with invalid key")
		}
		if s.Version < 1 {
			t.Fatal("validation passed with zero version")
		}
		if len(s.Steps) == 0 {
			t.Fatal("validation passed with no steps")
		}
		seen := make(map[string]bool)
		for _, step := range s.Steps {
			if step.ID == "" {
				t.Fatal("validation passed with empty step id")
			}
			if !allowedStepTypes[step.Type] {
				t.Fatal("validation passed with unknown step type")
			}
			if seen[step.ID] {
				t.Fatal("validation passed with duplicate step id")
			}
			seen[step.ID] = true
		}
		if s.Parameters.MaxEvents < 1 || s.Parameters.MaxEvents > 10000 {
			t.Fatal("validation passed with out-of-bounds max_events")
		}
	})
}
