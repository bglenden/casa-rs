---
name: wdad-project-board-setup
description: Use when creating or repairing the GitHub Issues/Projects planning surface for the methodology, including board fields, transition semantics, and automation support.
---

# Skill: Project board setup

## Purpose
Create or repair the GitHub planning surface used as the methodology source of truth.

## Mode
Planning mode is preferred. Edits may be made through `gh` only after the human approves the proposed structure.

## Procedure
1. Confirm the repo and owner.
2. Propose statuses: Backlog, Shaped, Current Wave, Implementing, Review, Stabilize, Done, Parked.
3. Propose fields: Horizon, Kind, Area, Risk, Needs ADR, Test Depth.
4. Propose minimal labels only where fields are insufficient.
5. Confirm `gh auth refresh -s project` if project commands are needed.
6. Create or update issue templates if missing.
7. Create a first wave issue only after the human confirms the project structure.

## Output
- Proposed board structure
- Required manual GitHub UI steps, if any
- `gh` commands, if safe and supported
- First wave issue draft, if requested

## Do not
- Create duplicate boards without checking existing ones
- Hand-maintain BACKLOG.md or WAVE.md
