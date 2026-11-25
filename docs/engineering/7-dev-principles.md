# Development principles

(Part of the [engineering guide](../ENGINEERING.md).)

- **KISS** — the minimal abstraction that is correct. **Test first** — the
  failing test, then the implementation.
- **Atomic conventional commits**, one logical change each: `feat(matrix): …`,
  `fix(isochrone): …`, `docs: …`.
- **Warnings are errors.** `[workspace.lints]` denies warnings, dead code,
  unused items and `unsafe_code`. The sole `unsafe` carveout is the
  memory-mapping surface in `route/src/formats/mmap.rs`, with SAFETY blocks.
- **No placeholders** — no stubs, no "implement later". If it cannot be done
  correctly, stop and ask.
- **Prove it.** Run the pipeline on Belgium, check the lock conditions, run the
  gate. Never assume a code path is live — verify the served artefact.
  **Profile before optimising**, and prefer a better algorithm to more threads.
