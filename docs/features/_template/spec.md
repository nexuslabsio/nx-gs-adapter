# <Feature name>

> 📝 File name — kebab-case based on the feature's meaning:
> `pdf-export.md`, not the ticket id.
> *Delete when filled in.*

> Owner: @...

## Problem

> 📝 What pain the feature solves and for whom. 2-3 sentences.
> WHAT + WHY, not HOW.
>
> Good: "Users share documents via screenshots and copy-paste into
> Word. They need a way to export a document into an immutable format."
>
> Bad: "Implement a REST endpoint that converts Document to PDF."
>
> Mark fuzzy phrasing with `[NEEDS CLARIFICATION: ...]`.
> *Delete when filled in.*

## Requirements

> 📝 Functional requirements with priorities and statuses.
>
> Wording:
> - Each R is testable: "done / not done".
> - Format: "System MUST ..." or "User MUST be able to ..."
> - In terms of behavior, not implementation.
    >   ✓ "R1. User MUST be able to export document to PDF"
    >   ✗ "R1. Add DocumentExporter class"
>
> Numbering:
> - Continuous across Must/Should/Could: R1, R2, R3, ...
> - Deleted numbers are not reused.
>
> Statuses: `[done]` / `[wip]` / `[todo]`. After `[wip]` add a single
> space to align with `[done]` and `[todo]`.
>
> Priorities (RFC 2119):
> - Must:      the feature doesn't make sense without it.
> - Should:    important, but the feature still makes sense without it.
> - Could:     a nice extension, makes the feature better.
> - Non-goals: deliberately out of scope for this feature.
>
> *Delete when filled in.*

**Must:**

- [todo] R1. System MUST ...

**Should:**

**Could:**

**Non-goals:**

> 📝 SC (success criteria) — optional measurable criteria under an R.
> Nested list under the R, only when a concrete threshold exists.
> Continuous numbering: SC1, SC2, ... Technology-agnostic.
> Status is carried by the parent R; SC itself has no status.
>
> Example:
> ```
> - [wip] R3. System MUST generate PDF in background for large docs
>   - SC1: sync generation completes within 10 sec for docs <100 pages
> ```
> *Delete when filled in.*

### Edge cases

> 📝 Edge and exceptional cases. "What if..." questions worth
> considering during implementation.
>
> - What happens if <condition>?
> - How does the system handle <error>?
>
> Delete the section entirely if empty.
> *Delete when filled in.*

## Open questions

> 📝 Questions to answer before/during implementation. Product AND
> technical uncertainty — all go here.
>
> Prefixes:
> - No prefix — a plain open question.
> - `[assumed: ...]` — an assumption made for lack of information;
    >   needs confirmation. Example: "[assumed: files up to 50 MB]".
> - `[NEEDS CLARIFICATION: ...]` — a critical question that blocks
    >   understanding or implementation. Must be resolved before work
    >   starts.
>
> Resolved questions are either deleted or marked
> "→ resolved: answer".
> *Delete when filled in.*

- [ ] ...

## Links (optional)

> 📝 External links: Jira, Figma, related specs, external APIs, ADRs.
> Delete the section if empty.
> *Delete when filled in.*

- Jira: <link>
