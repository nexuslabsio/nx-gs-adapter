# <Feature name> — tech

> 📝 Technical description of the feature. Created at any point in
> the feature's lifecycle:
> - Proactively at `-init` time, if the description includes tech
    >   details (tools, architectural decisions, integrations).
> - Retroactively at implementation time via `-sync tech`.
>
> Answers "how the feature is designed and wired", not "what it does"
> (that's spec.md) and not "how to build it right now" (that's .plan/).
>
> Audience: someone reading this doc six months from now, a new
> teammate, an LLM agent.
> *Delete when filled in.*

> 📝 The `[planned]` marker goes on individual list items that are
> designed but not yet implemented in code:
> - on components in Key components
> - on integrations in Integration points
> - on entities/tables in Data model
>
> NOT applied:
> - to entire sections (a section is either present or absent)
> - in Overview (describes the feature as a whole)
> - in Decisions (a decision exists from the moment it's made)
> - in Extension points (an architectural capability, not an
    > implementation)
>
> The marker is removed by `-sync tech` once the item is implemented.
> *Delete when filled in.*

> Covers: spec.md

## Overview

> 📝 2-3 sentences: the parts the feature consists of at the code
> level.
>
> Describes the feature as a whole, without `[planned]`. If parts
> aren't implemented yet — phrase it naturally ("generation is
> planned via a Celery worker").
>
> Good: "PDF export consists of a synchronous endpoint on
> DocumentsController, a background Celery worker, and an S3
> uploader. Background job status is tracked in the export_jobs
> table."
> *Delete when filled in.*

## Structure

> 📝 Where things live in the repo. Modules, files, directories + the
> role of each. Depth — down to the file/module, not the function.
>
> Format — a list of paths with short descriptions:
> - modules/export/ — all of the feature's code
> - modules/export/exporter.py — synchronous generation
>
> During the `[planned]` phase the section may be empty or contain
> intended paths marked `[planned]`:
> - modules/export/ [planned] — folder for the feature's code
    > *Delete when filled in.*

## Key components

> 📝 Core classes / services / modules. Name + one sentence about
> the role. Optionally — a link back to R from spec.md.
>
> Rules:
> - Only what matters for the big picture. Utils and helpers — no.
> - Don't duplicate docstrings from the code.
> - Code snippets — ONLY when the explanation is impossible without
    >   them. Max 10 lines per snippet, max 3 snippets per document.
> - Components that are designed but not yet implemented are marked
    >   `[planned]`.
>
> Good:
> - **PdfExporter** (implements R1, R2) — synchronous PDF generation.
    >   Blocking call, up to 100 pages.
> - **BackgroundGenerator** [planned] (implements R3) — Celery task
    >   for the async flow of large documents.
    > *Delete when filled in.*

## Data flows (optional)

> 📝 Optional section. Non-trivial interaction scenarios between
> components. Format: a short step-by-step description
> "request → ... → response".
>
> Mermaid diagrams — only when the text is more complex than the
> diagram. For linear processes A → B → C a list is enough.
>
> Delete the section if there are no flows or they're obvious.
> *Delete when filled in.*

## Data model (optional)

> 📝 Optional section. Only if the feature interacts with a database
> or has key data structures.
>
> Format:
> - Table / entity name
> - Key fields (not all — only the ones important for understanding
    > the role)
> - Link to a migration or ORM model
>
> The full schema is not duplicated — link the migrations.
> Tables/entities not yet created are marked `[planned]`.
>
> Good:
> - **export_jobs** [planned] — status of background export jobs.
    >   Key fields: status, result_url, user_id.
    > *Delete when filled in.*

## Integration points (optional)

> 📝 Optional section. Integrations with external systems and other
> modules in the project.
>
> Format:
> - Whom the integration is with
> - Where in the code the integration point lives
> - Briefly — how
>
> Integrations not yet wired up are marked `[planned]`.
>
> Good:
> - **S3** (bucket: documents-exports) [planned] — upload of
    >   generated PDFs, presigned URL generation.
    > *Delete when filled in.*

## Decisions (optional)

> 📝 Optional section. Non-trivial architectural decisions that
> answer "why this way and not otherwise?".
>
> Format:
> - Decision: what was done / is planned
> - Why: context + rejected alternatives
>
> The `[planned]` marker is NOT used — a decision exists from the
> moment it's made, regardless of implementation status.
>
> If the decision is truly important and was deliberated — a full
> ADR in docs/adr/ with a link from here.
> *Delete when filled in.*

## Extension points (optional)

> 📝 Optional section. Explicit extension points — where to look
> when adding a new variant / format / handler.
>
> Format:
> - What can be extended
> - Where the extension point lives in the code
> - How to add a new one
>
> The `[planned]` marker is NOT used — this describes an
> architectural capability, not a specific component.
>
> Delete the section if the feature isn't meant to be extended.
> *Delete when filled in.*
