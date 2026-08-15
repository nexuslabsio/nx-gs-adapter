# <Feature name>

> 📝 Copy this file to `docs/specs/NNN-<feature>.md` — `NNN` is the next free sequential id, the slug
> is kebab-case and names the feature's meaning (`pdf-export`), not a ticket id. Add the index row in
> `docs/CLAUDE.md` in the same pass. Grow to a folder (`NNN-<feature>/spec.md` + companions) only if
> the feature genuinely needs a second document.
> _Delete this block when filled in._

> Owner: @...

## Problem

> 📝 What pain the feature solves and for whom. 2-3 sentences. WHAT + WHY, not HOW.
>
> Good: "Users share documents via screenshots and copy-paste into Word. They need a way to export a
> document into an immutable format."
>
> Bad: "Implement a REST endpoint that converts Document to PDF."
>
> Mark fuzzy phrasing with `[NEEDS CLARIFICATION: ...]`.
> _Delete when filled in._

## Requirements

> 📝 Functional requirements with priorities and statuses.
>
> Wording:
>
> - Each R is testable: "done / not done".
> - Format: "System MUST ..." or "User MUST be able to ..."
> - In terms of behavior, not implementation.
>   ✓ "R1. User MUST be able to export document to PDF"
>   ✗ "R1. Add DocumentExporter class"
>
> Numbering: continuous across Must/Should/Could (R1, R2, R3, ...); deleted numbers are not reused.
>
> Statuses: `[done]` / `[wip]` / `[todo]`. After `[wip]` add a single space to align with the others.
>
> Priorities (RFC 2119):
>
> - Must: the feature doesn't make sense without it.
> - Should: important, but the feature still makes sense without it.
> - Could: a nice extension, makes the feature better.
> - Non-goals: deliberately out of scope for this feature.
>
> _Delete when filled in._

**Must:**

- [todo] R1. System MUST ...

**Should:**

**Could:**

**Non-goals:**

> 📝 SC (success criteria) — optional measurable criteria under an R. Nested list, only when a
> concrete threshold exists. Continuous numbering: SC1, SC2, ... Technology-agnostic. Status is
> carried by the parent R; an SC has none.
>
> ```
> - [wip] R3. System MUST generate PDF in background for large docs
>   - SC1: sync generation completes within 10 sec for docs <100 pages
> ```
>
> _Delete when filled in._

### Edge cases

> 📝 Edge and exceptional cases — "what if..." questions worth considering during implementation.
> Delete the section entirely if empty.
> _Delete when filled in._

## Technical design

> 📝 How the feature is designed and wired — not what it does (Requirements above) and not how to
> build it right now (that's a `.plan/`, which is gitignored and never committed).
>
> Mark items that are designed but not yet implemented with `[planned]` — on components,
> integrations, and data-model entries. Not on whole sections, not in Decisions (a decision exists
> from the moment it is made).
> _Delete when filled in._

### Overview

> 📝 2-3 sentences: the parts the feature consists of at the code level.
> _Delete when filled in._

### Structure

> 📝 Where things live in the repo — modules, files, directories + the role of each. Down to the
> file/module, not the function.
> _Delete when filled in._

### Key components

> 📝 Core classes / services / modules: name + one sentence on its role, optionally linking back to
> an R. Only what matters for the big picture — no utils, no docstring copies. Code snippets only
> when the explanation is impossible without them (max 10 lines, max 3 per document).
> _Delete when filled in._

### Data flows (optional)

> 📝 Non-trivial interaction scenarios: "request → ... → response". Mermaid only when text would be
> harder to follow than the diagram. Delete if the flows are obvious.
> _Delete when filled in._

### Data model (optional)

> 📝 Only if the feature owns tables or key data structures. Name + the fields that matter + a link
> to the migration / model. Never duplicate the full schema.
> _Delete when filled in._

### Integration points (optional)

> 📝 Integrations with external systems and other modules: with whom, where in the code, briefly how.
> _Delete when filled in._

### Decisions (optional)

> 📝 Non-trivial choices answering "why this way and not otherwise?" — decision + context + rejected
> alternatives. This is the part of a spec that ages best; be generous here.
> _Delete when filled in._

### Extension points (optional)

> 📝 Where to plug in next — the seams left deliberately open.
> _Delete when filled in._

## Rollout (optional)

> 📝 For anything touching the wire: release ordering across repos, the two-release deprecation gates
> (see the root `CLAUDE.md`), and what must be deployed before what.
> _Delete when filled in._

## Open questions

> 📝 Product AND technical uncertainty. Prefixes: none = plain open question;
> `[assumed: ...]` = assumption made for lack of information, needs confirmation;
> `[NEEDS CLARIFICATION: ...]` = blocks implementation, must be resolved before work starts.
> Resolved questions are deleted or marked "→ resolved: answer".
> _Delete when filled in._

- [ ] ...

## Links (optional)

> 📝 GitHub issues, counterpart specs in other repos (repo-qualified paths — filesystem-relative
> links do not resolve across repos on GitHub), external APIs. Delete the section if empty.
> _Delete when filled in._
