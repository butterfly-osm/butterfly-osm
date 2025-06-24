**XP Pair Programming: Human + AI (Rust Edition)**

> **When in doubt, ask. Never assume. KISS at all times.**

---

## Core Cycle

1. **Explore → Plan → Feature Branch → Test → Code → Refactor → Document → Changelog → Pull Request**
2. **Docker Compose-first**

   * Run every component (app, database, tests) in containers—even during development.
   * Always run docker compose through the Makefile for repeated commands.
3. **Test-first**

   * Write a failing test, then implement just enough to make it pass.
4. **Simplest design**

   * Always choose the minimal abstraction that works.
5. **Meaningful naming**

   * Names for modules, functions, types, and variables should read like clear prose.
6. **Atomic commits**

   * One logical change per commit; keep tests passing before merging.
7. **Courage to change**

   * Refactor fearlessly under comprehensive test coverage.
8. **Continuous dialogue**

   * Ask questions, share context, iterate rapidly.
9. **Collective ownership**

   * Any team member—human or AI—can modify any part of the codebase.
10. **One source of truth**

    * The Git repository is authoritative.
11. **Documentation in sync**

    * Keep code comments, README, and CHANGELOG aligned with implementation.
12. **README.md**

    * Clearly state: What, Why, How, and Who.
13. **Patience**

    * Allow builds, downloads, and containers to finish without interruption.

---

## Collaboration Rules

* **Think aloud**

  * Articulate design choices in comments or PR descriptions.
* **Show work**

  * Share diffs before large refactors; annotate non-trivial changes.
* **Stay focused**

  * Clear context when switching tasks.
* **Be explicit**

  * Define constraints, acceptance criteria, and ask clarifying questions.
* **Iterate**

  * Refine features through multiple review rounds.

---

## Git Flow

1. **Issue → Branch**

   * Reference or create an issue; branch naming: `feature/short-description`.
2. **Develop**

   * Test → implement → verify.
3. **Pull Request**

   * Link to the issue; include tests and documentation updates.
   * Use Conventional Commits (e.g. `feat(module): add …`).
4. **Review & Merge**

   * Address feedback; ensure CI passes; merge cleanly.
5. **Release**

   * Update CHANGELOG; tag with semantic version; build artifacts.