# Deep Dive: MkDocs Material Documentation Site + GitHub Pages CI

**Generated**: 2026-08-15
**Phase**: Documentation site setup (commit `ab9d310`)
**Files**: `mkdocs.yml`, `.github/workflows/docs.yml`, `docs/` (15 pages + nav structure), `Makefile` (docs targets), `.gitignore`

---

## Overview

### What This Code Does

Three things bolted together: (1) `mkdocs.yml` — a declarative config that tells the MkDocs static-site generator how to theme, structure, and render `docs/*.md` into HTML; (2) `docs/` itself — content organized by the Diataxis framework (tutorial / how-to / reference / explanation) rather than by mirroring the code's package layout; (3) `.github/workflows/docs.yml` — a two-job GitHub Actions pipeline that builds that site and publishes it to GitHub Pages on every push to `main`.

None of this touches the Go binary. It's a parallel, purely additive pipeline — nothing in `cmd/` or `internal/` depends on it.

### Why This Approach Was Chosen

The alternative to a static-site generator is hand-written HTML or just shipping `README.md`/`AGENT.md` as-is. Both scale badly once you have >1 page: no shared theme, no cross-page search, no consistent nav. MkDocs solves this by treating docs as data (a `nav:` tree + a directory of Markdown) and a theme as a separate, swappable concern — you're not hand-maintaining `<nav>` HTML across 15 files.

Diataxis was chosen over "one big README" because this project already had a README that mixed a quickstart, an API table, and an architecture narrative in one linear scroll — readable once, hard to scan for "just the API" or "just how consumer groups work" later. Splitting by *reader intent* (I want to get started / I want to do X / I want to look up Y / I want to understand why) rather than by *code structure* is the core bet of that framework.

### Context

Use this pattern when a project has enough surface area that a single README becomes a junk drawer — multiple entry points (CLI, HTTP API, architecture), and readers who arrive wanting different things (a first-time user vs. someone debugging a specific endpoint).

---

## Code Walkthrough

### File 1: `mkdocs.yml`

**Purpose**: The single source of truth for how the site is built — theme, nav, and the Markdown parser's extension pipeline.

**Key sections**:

```yaml
theme:
  name: material
  palette:
    - media: "(prefers-color-scheme: light)"
      scheme: default
      toggle: { icon: material/brightness-7, name: Switch to dark mode }
    - media: "(prefers-color-scheme: dark)"
      scheme: slate
      toggle: { icon: material/brightness-4, name: Switch to light mode }
```

Two `palette` entries, not one. Each is gated by a `media` query matching the OS preference *and* carries a `toggle` the reader can click to override it. Material's JS persists that override in `localStorage`, so "system default, but overridable" is the resulting behavior — neither entry alone would give you that; you need the pair plus the toggle wiring.

```yaml
  features:
    - navigation.tabs
    - content.code.copy
    - toc.follow
    # ...
```

Every visible "polish" feature (copy buttons, sticky nav, search highlighting) is an opt-in string here, not a plugin to install — Material ships all of them and gates them behind this list. Nothing to add to `requirements.txt` for any of these.

```yaml
markdown_extensions:
  - pymdownx.superfences:
      custom_fences:
        - name: mermaid
          class: mermaid
          format: !!python/name:pymdownx.superfences.fence_code_format
```

This is the one non-obvious piece. `pymdownx.superfences` is a generic "fenced code block" extension; the `custom_fences` entry teaches it that a ` ```mermaid ` block isn't code to syntax-highlight — it's a `<pre class="mermaid">` for Material's bundled Mermaid.js to find and render client-side. No `mkdocs-mermaid2-plugin` or similar needed; this is the officially documented zero-extra-dependency route.

```yaml
nav:
  - Home: index.md
  - Tutorials: [...]
  - How-To Guides: [...]
  - Reference: [...]
  - Explanation: [...]
```

`nav:` is listed explicitly rather than auto-generated from the directory tree. Combined with `navigation.tabs`, these five top-level entries become clickable tabs — the nav structure *is* the Diataxis quadrant selector in the rendered UI, not just an organizational convention in the source tree.

### File 2: `.github/workflows/docs.yml`

**Purpose**: Build the site and publish it to GitHub Pages on every push to `main`.

**Key components**:

```yaml
permissions:
  contents: read
  pages: write
  id-token: write

concurrency:
  group: pages
  cancel-in-progress: false
```

`id-token: write` is the tell that this uses OIDC (OpenID Connect) rather than a personal access token or the default `GITHUB_TOKEN` pushing to a branch. `concurrency.group: pages` means if two pushes land close together, the second run queues rather than racing the first to publish — `cancel-in-progress: false` specifically means it *waits* rather than killing the in-flight deploy, so you never end up serving a half-deployed site.

```yaml
jobs:
  build:
    steps:
      - uses: actions/setup-python@v5
        with: { python-version: "3.12", cache: pip }
      - run: pip install -r requirements.txt
      - run: mkdocs build --strict
      - uses: actions/upload-pages-artifact@v3
        with: { path: site }

  deploy:
    needs: build
    environment: { name: github-pages, url: ${{ steps.deployment.outputs.page_url }} }
    steps:
      - id: deployment
        uses: actions/deploy-pages@v4
```

Two jobs, not one, on purpose: `build` produces an artifact and has no deploy permissions beyond `pages: write`; `deploy` consumes that artifact via the `github-pages` **environment**, which is where GitHub's Pages-specific OIDC trust and the resulting public URL live. `mkdocs build --strict` is the actual quality gate here — it turns any broken internal link or nav reference into a build failure instead of a silently broken page in production.

### File 3: `docs/` (structural)

**Purpose**: 15 content pages, none co-located with the code they document — grouped by reader intent under `tutorials/`, `how-to/`, `reference/`, `explanation/` instead of mirroring `cmd/`/`internal/`.

**Key components**:
- `docs/index.md` — landing page with card-grid links into each quadrant (Material's `grid cards` — pure CSS via `attr_list`/`md_in_html`, no extra plugin).
- `docs/tutorials/getting-started.md` — the one prescribed golden path (infra-up → service → simulate → fraudctl), no branching.
- `docs/reference/*.md` — dry tables only (flags, endpoints, metric names), cross-checked line-by-line against the actual Go source rather than paraphrased from the README, so flag names/defaults/env vars can't drift silently from what `cmd/fraud-service/main.go` actually parses.
- `docs/explanation/*.md` — the only place prose narrative and "why" live; how-to and reference pages link out to these instead of re-explaining.

---

## Concepts Explained

### Design Patterns Used

| Pattern | Where | Why |
|---------|-------|-----|
| Config-as-data (declarative site config) | `mkdocs.yml` | Nav/theme/parser behavior is one reviewable file, not scattered template logic |
| Content-type separation (Diataxis) | `docs/` directory layout + `nav:` | Matches structure to reader intent, not to code structure |
| Two-stage pipeline with an artifact handoff | `docs.yml` `build` → `deploy` | Least-privilege: only `deploy` touches the Pages environment/OIDC trust |
| Fail-fast quality gate | `mkdocs build --strict` | Turns broken links into CI failures instead of silent 404s in production |

### Key Technical Concepts

#### Diataxis documentation framework

**What**: A model that says documentation serves exactly four distinct reader needs — learning (tutorial), doing (how-to), looking up (reference), understanding (explanation) — and that mixing two of these on one page serves neither well.

**Why Used Here**: The project's README before this change mixed all four (quickstart = tutorial, API table = reference, architecture narrative = explanation) in one linear document. Splitting by intent instead of letting content accrete on one page is exactly the failure mode Diataxis names.

**When to Use**: Once a project has enough content that "where do I put this new paragraph" stops being obvious. For a single-page README on a small project, it's overkill — the split earns its complexity only past a certain size.

**Trade-offs**:
- Pros: readers can jump straight to the quadrant matching their current need; each page type has a narrow, checkable definition of "done."
- Cons: more files, more nav structure to maintain, some inevitable near-duplication (a How-To and its underlying Explanation page will reference the same mechanism from different angles).

**Alternatives**:
- Single README, sectioned by `##` headers: fine up to a handful of sections; degrades once a reader needs to scroll past unrelated content to find their section.
- Docusaurus/Sphinx-style "just a tree of pages" with no framework: same tooling category as MkDocs, but without an opinion on *how* to split content, the split tends to re-drift toward "whatever seemed convenient" over time.

**Prerequisites to understand this**:
- Markdown: the source format every page is written in.
- Static site generators generally: the idea that Markdown + a template produces a full site, no server-side rendering per request.

#### Static site generator config-as-code (MkDocs)

**What**: `mkdocs.yml` fully describes the build — MkDocs reads it, reads `docs/*.md`, and emits static HTML/CSS/JS into `site/`. There's no build script beyond `mkdocs build`.

**Why Used Here**: A Go project has no existing Python/JS toolchain to lean on, and doesn't need one for docs — MkDocs's only dependency is Python + pip, isolated to `requirements.txt`, and it never touches `go.mod`.

**When to Use**: Markdown-first documentation sites where you want a themed, searchable, navigable site without hand-writing HTML. Less suited to docs that need API-reference autogeneration from source (e.g., Python docstrings via `mkdocstrings`, deliberately *not* used here since this is a Go codebase with nothing for a Python autodoc plugin to introspect).

**Trade-offs**:
- Pros: config is one diffable file; theme upgrades are a `pip install --upgrade` away; `mkdocs serve` gives live-reload local preview.
- Cons: yet another toolchain (Python/pip) alongside Go's, even if only for CI and optional local preview.

**Alternatives**:
- Docusaurus (React/Node-based): heavier toolchain, better suited to projects already in the JS ecosystem or wanting MDX/React components in docs.
- Hugo: faster builds, Go-based (would match this project's language) but a steeper templating learning curve and no equivalent to Material's out-of-the-box theme polish.
- Plain GitHub-rendered Markdown (no site at all): zero setup, but no search, no nav, no theme, no custom domain.

**Prerequisites to understand this**:
- YAML: the format `mkdocs.yml` is written in.
- Static vs. dynamic sites: the site is pre-built HTML, not rendered per-request by a server.

#### OIDC-based GitHub Pages deployment (`actions/deploy-pages`)

**What**: Instead of the classic pattern (build the site, `git push` it to a `gh-pages` branch, configure Pages to serve that branch), the workflow uploads a build artifact and a separate `deploy` job exchanges a short-lived OIDC token for permission to publish it to Pages directly — no extra branch involved.

**Why Used Here**: Fewer moving parts (no `gh-pages` branch to keep in sync, no risk of someone editing generated HTML directly on that branch) and narrower credentials (`id-token: write` mints a token scoped to this one deployment, versus a `GITHUB_TOKEN` with push rights to a branch).

**When to Use**: Any GitHub Pages deployment where you don't have another consumer relying on the `gh-pages` branch existing (some older tools/badges assumed that branch as their integration point).

**Trade-offs**:
- Pros: first-class GitHub "Environment" (`github-pages`) with deployment history and a URL surfaced directly in the Actions UI and PR checks; least-privilege by construction.
- Cons: requires the one-time manual repo setting (Settings → Pages → Source → "GitHub Actions") — cannot be set via a file in the repo, which is exactly the step this session needed the repo owner to do by hand before the first `deploy` job could succeed.

**Alternatives**:
- `mkdocs gh-deploy` / `peaceiris/actions-gh-pages`: pushes to `gh-pages` branch. Simpler mental model, but broader token scope and an extra branch to reason about.
- Third-party static hosts (Netlify, Vercel, Cloudflare Pages): more features (preview deployments per PR) at the cost of an external account/service outside GitHub.

**Prerequisites to understand this**:
- GitHub Actions jobs/steps/permissions: the base mechanics being configured here.
- OIDC (OpenID Connect) in brief: a token exchange where GitHub's Actions runner proves its identity to get a short-lived credential, instead of a long-lived secret being stored anywhere.

#### `pymdownx.superfences` custom fences (Mermaid diagrams)

**What**: A Markdown extension that lets you register a fenced-code-block language name (` ```mermaid `) to be treated as an embeddable non-code block rather than syntax-highlighted code — Material's bundled JS then finds every `<pre class="mermaid">` on the page and renders it as an SVG diagram in the browser.

**Why Used Here**: The project's existing architecture diagram was ASCII art in `README.md`/`AGENT.md` — fine in a terminal, static and non-interactive on a themed web page. Converting it to Mermaid *only* in `docs/explanation/architecture-overview.md` (leaving the ASCII untouched in README/AGENT.md, which are read via `cat`/terminal/agent context where Mermaid can't render) gets a properly arrowed, theme-aware diagram exactly where it's actually rendered.

**When to Use**: Any docs site with an SSG that supports it, for diagrams that benefit from being data (text you can diff and edit) rather than a checked-in image file.

**Trade-offs**:
- Pros: diagrams are plain text, diffable in PRs, no image asset to regenerate and re-upload.
- Cons: renders client-side via JS — a reader with JS disabled sees the raw Mermaid source; more limited layout control than hand-drawn/exported diagrams for complex topologies.

**Prerequisites to understand this**:
- Markdown fenced code blocks (` ``` `): the base syntax being repurposed.
- Mermaid syntax: a small text DSL for flowcharts/sequence diagrams/etc.

---

## Learning Resources

### Official Documentation

- [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/): the theme's full reference — every `features:` flag, palette option, and extension used in `mkdocs.yml` is documented here.
- [MkDocs](https://www.mkdocs.org/): the underlying static-site generator's own docs — `nav:`, plugin system, `mkdocs build`/`serve` CLI.
- [Diataxis](https://diataxis.fr/): the documentation framework driving the `docs/` split; short and worth reading end-to-end (~20 min).
- [GitHub Actions: Publishing with a custom GitHub Actions workflow](https://docs.github.com/en/pages/getting-started-with-github-pages/configuring-a-publishing-source-for-your-github-pages-site#publishing-with-a-custom-github-actions-workflow): the `actions/upload-pages-artifact` + `actions/deploy-pages` pattern used here, from GitHub itself.
- [Mermaid](https://mermaid.js.org/): syntax reference for the flowchart used in `architecture-overview.md`.

### Related Concepts (For Deeper Study)

- GitHub Environments: what `environment: { name: github-pages }` actually is and how deployment protection rules work on them.
- `pymdown-extensions` full catalog: the project only turned on a subset (`admonition`, `tabbed`, `details`, `snippets`, `emoji`, `highlight`) — worth skimming the rest.
- Versioned docs (e.g., the `mike` plugin): relevant if this project ever needs to publish docs for multiple released versions side by side — deliberately *not* set up here since there's only ever one version of this project live at a time.

---

## Related Code in This Project

| File | Relationship |
|------|--------------|
| `Makefile` (`docs-serve`, `docs-build`) | Local equivalents of what CI runs — `docs-build` runs the exact `mkdocs build --strict` the workflow uses, so a CI failure is reproducible locally first. |
| `requirements.txt` | Pins `mkdocs-material`/`pymdown-extensions` version ranges consumed by both the Makefile targets and the CI workflow's `pip install -r requirements.txt`. |
| `.gitignore` (`/site/`, `.venv/`) | `site/` is `mkdocs build`'s output directory — never committed; CI rebuilds it fresh every run. |
| `README.md` / `AGENT.md` | Source material the `docs/` prose was drawn from and cross-checked against; their ASCII architecture diagram is the one the Mermaid version in `explanation/architecture-overview.md` replaces *for the docs site only*. |

---

## Next Steps

1. **Try it yourself**: `make docs-serve`, then edit any `docs/*.md` file and watch the browser live-reload. Toggle light/dark in the header to see the `palette` config in action.
2. **Deeper dive**: read `docs/reference/configuration.md` side-by-side with `cmd/fraud-service/main.go` — every flag in that table has a one-line source in the Go file it documents; this is the discipline that keeps a Reference page from drifting out of sync with the code.
3. **Common pitfalls**:
   - `repo_url`/`site_url` in `mkdocs.yml` are just strings — they don't validate against the actual GitHub remote. This session's ownership transfer to `BigBenCodes` required manually editing both; a stale `repo_url` after a fork/rename/transfer is an easy thing to miss since nothing fails loudly.
   - `mkdocs build --strict` only catches *internal* broken links (nav entries, cross-page references) — it won't catch a typo'd external URL. Consider a link-checker step if the docs start linking out more.

---

*This deep dive was generated by AntiVibe - the anti-vibecoding learning framework.*
*Learn what AI writes, not just accept it.*
