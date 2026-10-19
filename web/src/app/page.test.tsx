import { expect, test } from "vitest";
import { render, screen } from "@testing-library/react";
import Home from "./page";

test("home page identifies the workspace and reports that execution is unavailable", () => {
  render(<Home />);

  expect(
    screen.getByRole("heading", { name: "Event reliability testing" }),
  ).toBeDefined();
  expect(screen.getByRole("status").textContent).toBe(
    "Scenario execution is not available yet.",
  );
});
