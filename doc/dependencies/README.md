# Dependency extraction per-language

This folder contains language-specific notes on how repository-level dependencies are statically extracted and normalized.

Current entries
- `gradle.md` — Gradle (Groovy + Kotlin DSL) extraction and catalog resolution.

Planned entries (examples)
- `maven.md` — Maven `pom.xml` static parsing notes (if added).
- `nodejs.md` — `package.json` / `package-lock.json` / `yarn.lock` extraction notes.
- `python.md` — `requirements.txt` / `pyproject.toml` handling.

Guidance for adding language docs
- Create a `doc/dependencies/<language>.md` file describing:
  - files scanned
  - supported syntax patterns
  - limitations
  - where code lives in the repo
