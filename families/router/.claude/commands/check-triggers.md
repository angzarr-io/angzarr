---
description: Review Deferred tasks and surface any whose revive trigger has fired
---


Review the project's Deferred tasks and decide which ones their **revive
trigger** says should come back now. A Deferred task is one parked on a named
condition (the same idea as an ADR revive trigger): it stays out of the active
list until that condition is met.

## How to run it

1. **List the Deferred tasks.** Call the `task_list` MCP tool with
   `statuses: ["Deferred"]` (Deferred tasks are hidden from the default view, so
   you must name the status). Each task carries a `trigger` field — the
   free-text condition that should revive it. If there are none, say so and stop.

2. **Evaluate each trigger against the current state of the world.** You are the
   judge, exactly as a human would be for an ADR revive trigger. Read the
   trigger and gather whatever evidence bears on it — the repository, recent
   commits and releases, open work, the conversation so far, dates, CI/deploy
   status, whatever the trigger references. For each Deferred task decide:
   **fired**, **not yet**, or **unsure**, and write one sentence of reasoning
   citing the evidence.

3. **Report — do not act yet.** Present a short list: for each task its harp id,
   text, trigger, your verdict, and the reason. Lead with the ones you judge
   fired.

4. **Confirm before moving anything.** Ask the user which of the fired tasks to
   revive. Only for the ones they confirm, call `task_set_status` with
   `status: "To Do"` (no trigger needed — moving off Deferred preserves the
   stored trigger). Leave "not yet" and "unsure" tasks Deferred. Never move a
   task back automatically; the transition is the user's call.

## Notes

- Triggers are deliberately free text — there is no machine evaluation. Your
  judgement is the mechanism. When unsure, say so rather than guessing; an
  unsure verdict keeps the task parked.
- This is the read/decide half of the Deferred workflow. The defer half is
  `task_set_status` with `status: "Deferred"` and a `trigger`, or `task_add`
  with the same.
