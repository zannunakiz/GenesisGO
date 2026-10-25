import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, expect, test, vi } from "vitest";
import Home from "./page";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

test("home page shows the empty scenario state", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ scenarios: [] }),
    } as Response),
  );

  render(<Home />);

  expect(
    screen.getByRole("heading", { name: "Event reliability testing" }),
  ).toBeDefined();
  expect(screen.getByLabelText("Scenario")).toBeDefined();
  expect(screen.getByRole("status").textContent).toBe("Loading scenarios...");
  expect(await screen.findByText("No scenarios are available.")).toBeDefined();
  expect(screen.getByRole("button", { name: /Run scenario/ })).toBeDefined();
});

test("authentication error accepts a key and retries scenarios", async () => {
  const fetchMock = vi
    .fn()
    .mockResolvedValueOnce({
      ok: false,
      status: 401,
    } as Response)
    .mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({ scenarios: [] }),
    } as Response);
  vi.stubGlobal("fetch", fetchMock);

  render(<Home />);
  const keyInput = await screen.findByLabelText("API key");
  fireEvent.change(keyInput, { target: { value: "local-test-key" } });
  fireEvent.click(screen.getByRole("button", { name: "Connect" }));

  await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
  expect(fetchMock.mock.calls[1][1]).toMatchObject({
    headers: { Authorization: "Bearer local-test-key" },
  });
});

test("run scenario submits the selected fixture and shows API status", async () => {
  const fetchMock = vi
    .fn()
    .mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        scenarios: [
          {
            key: "duplicate_delivery",
            version: 1,
            name: "Duplicate Delivery",
            description: "Checks duplicate handling.",
            parameters: {
              max_events: 5,
              max_retries: 3,
              timeout_ms: 15000,
              concurrency: 1,
            },
          },
        ],
      }),
    } as Response)
    .mockResolvedValueOnce({
      ok: true,
      status: 201,
      json: async () => ({
        run: {
          id: "run-12345678",
          scenario_id: "scenario-1",
          status: "pending",
          attempt_count: 0,
          version: 1,
          created_at: "2026-10-25T00:00:00Z",
        },
      }),
    } as Response);
  vi.stubGlobal("fetch", fetchMock);

  render(<Home />);
  fireEvent.click(await screen.findByRole("button", { name: "Run scenario" }));

  expect(await screen.findByRole("heading", { name: "Run run-1234" })).toBeDefined();
  expect(screen.getByText("pending")).toBeDefined();
  await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
  expect(fetchMock.mock.calls[1][0]).toBe("/api/v1/runs");
  expect(fetchMock.mock.calls[1][1]).toMatchObject({
    method: "POST",
    body: JSON.stringify({ scenario_key: "duplicate_delivery" }),
  });
});

test("report shows ordered evidence and Run again creates a new run", async () => {
  const fetchMock = vi
    .fn()
    .mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        scenarios: [
          {
            key: "duplicate_delivery",
            version: 1,
            name: "Duplicate Delivery",
            description: "Checks duplicate handling.",
            parameters: {
              max_events: 5,
              max_retries: 3,
              timeout_ms: 15000,
              concurrency: 1,
            },
          },
        ],
      }),
    } as Response)
    .mockResolvedValueOnce({
      ok: true,
      status: 201,
      json: async () => ({
        run: {
          id: "run-report-1",
          scenario_id: "scenario-1",
          status: "completed",
          attempt_count: 2,
          version: 3,
          created_at: "2026-10-25T00:00:00Z",
        },
      }),
    } as Response)
    .mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: async () => ({
        schema_version: "1.0.0",
        run: {
          id: "run-report-1",
          scenario_id: "scenario-1",
          status: "completed",
          attempt_count: 2,
          version: 3,
        },
        scenario: {
          key: "duplicate_delivery",
          name: "Duplicate Delivery",
          description: "Checks duplicate handling.",
        },
        timestamps: { created_at: "2026-10-25T00:00:00Z", updated_at: "2026-10-25T00:01:00Z" },
        event_attempts: [
          {
            id: "attempt-2",
            event_id: "evt-1",
            attempt_number: 2,
            delivered_at: "2026-10-25T00:00:20Z",
            fault_injected: "duplicate",
          },
          {
            id: "attempt-1",
            event_id: "evt-1",
            attempt_number: 1,
            delivered_at: "2026-10-25T00:00:10Z",
            acknowledged_at: "2026-10-25T00:00:11Z",
          },
        ],
        fault_points: [
          {
            step_id: "step-2",
            event_id: "evt-1",
            attempt_number: 2,
            fault_type: "duplicate",
            delivered_at: "2026-10-25T00:00:20Z",
          },
        ],
        side_effect_diff: [
          {
            step_id: "step-1",
            event_id: "evt-1",
            aggregate_id: "order-1",
            before: {
              snapshot_type: "before",
              aggregate_id: "order-1",
              state_summary: "eyJlZmZlY3RfY291bnQiOjB9",
              created_at: "2026-10-25T00:00:10Z",
            },
            after: {
              snapshot_type: "after",
              aggregate_id: "order-1",
              state_summary: "eyJlZmZlY3RfY291bnQiOjF9",
              created_at: "2026-10-25T00:00:11Z",
            },
          },
        ],
        invariant_results: [
          {
            invariant_key: "no_duplicate_effect",
            rule: "one effect per event",
            verdict: "fail",
            expected: "one effect",
            actual: "two effects",
            evidence: ["attempt-1", "attempt-2"],
            evaluated_at: "2026-10-25T00:01:00Z",
          },
        ],
        diagnosis: {
          summary: "The duplicate delivery may have repeated an effect.",
          suggestions: "WyJjaGVjayBpZGVtcG90ZW5jeSJd",
          model: "local-test-model",
          created_at: "2026-10-25T00:01:01Z",
          source: "ollama",
        },
      }),
    } as Response)
    .mockResolvedValueOnce({
      ok: true,
      status: 201,
      json: async () => ({
        run: {
          id: "run-report-2",
          scenario_id: "scenario-1",
          status: "pending",
          attempt_count: 0,
          version: 1,
          created_at: "2026-10-25T00:02:00Z",
        },
      }),
    } as Response);
  vi.stubGlobal("fetch", fetchMock);

  render(<Home />);
  fireEvent.click(await screen.findByRole("button", { name: "Run scenario" }));

  expect(await screen.findByRole("heading", { name: "Run report" })).toBeDefined();
  expect(screen.getByText("duplicate")).toBeDefined();
  expect(screen.getByText("Acknowledged")).toBeDefined();
  expect(screen.getByText("Not acknowledged")).toBeDefined();
  expect(screen.getAllByText(/effect_count/)).toHaveLength(2);
  expect(screen.getByText("fail")).toBeDefined();
  expect(screen.getByText("two effects")).toBeDefined();
  expect(screen.getByText("AI-assisted diagnosis")).toBeDefined();
  const timelineRows = within(screen.getByRole("table", { name: "Event timeline" })).getAllByRole("row");
  expect(timelineRows[1].textContent).toContain("1");
  expect(timelineRows[2].textContent).toContain("2");
  expect(fetchMock.mock.calls[2][0]).toBe("/api/v1/runs/run-report-1/report");

  fireEvent.click(screen.getByRole("button", { name: "Run again" }));
  expect(await screen.findByRole("heading", { name: "Run run-repo" })).toBeDefined();
  expect(fetchMock.mock.calls[3][0]).toBe("/api/v1/runs");
  expect(fetchMock.mock.calls[3][1]).toMatchObject({
    method: "POST",
    body: JSON.stringify({ scenario_key: "duplicate_delivery" }),
  });
});
