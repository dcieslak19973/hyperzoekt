# Gradle dependency handling (Groovy + Kotlin DSL)

This document describes how HyperZoekt statically extracts dependencies declared in Gradle build files.

What we parse
- `build.gradle` (Groovy DSL) — parsed with `tree-sitter-groovy`.
- `build.gradle.kts` (Kotlin DSL) — treated as Kotlin-language tagged files; extraction uses pattern matching rather than executing the Kotlin DSL.

Recognized dependency syntaxes
- Literal string notation:

  implementation 'com.example:library:1.2.3'

- Named-argument notation:

  implementation(group: 'com.example', name: 'library', version: '1.2.3')

- Project dependencies:

  implementation project(':submodule')

- Version catalog aliases:

  implementation libs.foo.bar

  When a `libs.versions.toml` file is present at the repository root or at `gradle/libs.versions.toml`, we parse it for entries of the form:

  - `foo = "group:name:version"` or `module = "group:name[:version]"` entries
  - table forms where `group`, `name`, and `version` appear as keys
  - `version.ref` references to a `[versions]` table entry

  The extractor resolves `libs.xxx` aliases to concrete `group:name[:version]` when possible and attaches the known `version` if present.

Language tagging
- Resulting dependencies are normalized to the repo language as follows:
  - `.kts` files produce dependencies tagged `kotlin`.
  - `.gradle` (Groovy) files produce dependencies tagged `java` (or `groovy` if you prefer, but `java` is used for compatibility in the current collector).

Limitations
- The parser only handles common static forms; dynamic constructs (interpolated variables, script plugins, or heavy Kotlin DSL usage) may not be recognized.
- Kotlin DSL AST recognition is limited. If you rely heavily on Kotlin-based build logic, generating a BOM or external manifest may be more reliable.

Where the code lives
- Tree-sitter based extraction: `crates/zoekt-rs/src/typesitter.rs` (function `extract_gradle_dependencies_typesitter`).
- Version catalog parsing and alias resolution: `crates/hyperzoekt/src/repo_index/deps.rs` (function `parse_gradle_version_catalog`).

Extending support
- Add additional regex patterns or AST checks in `typesitter.rs` to capture new Gradle forms.
- If you need semantics beyond static parsing (for example resolved dependency graphs including transitive dependencies), generate the dependency list using Gradle itself (for example `./gradlew dependencies` or `gradle dependencyInsight`) and import that output into HyperZoekt via a pre-processing step.
