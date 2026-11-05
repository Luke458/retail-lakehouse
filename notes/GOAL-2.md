# Goal 2: close out the flagship (after Weeks 1-6)

Repo `~/Projects/retail-lakehouse`. The first loop (`notes/GOAL.md`) built
Weeks 3-6 and ended with questions for the author. On 2026-11-05 the author
said to answer them myself ("answer the rest of the questions on ur own"),
and to run the next loop. Each iteration:
1. Read this file and do the first unchecked task, within the guardrails.
2. Tick it and add one line to the log at the bottom.
3. Continue. Stop when every task is ticked, or the rest are blocked.

Claude writes all the code, at the author's request.

## Answers to the first loop's questions (2026-11-05)

Each decision and its reason also goes in `notes/decisions.md` ("Close-out").

1. **Merge the week branches into `main`?** Yes. Fast-forward `main` to
   `week-06`: the branches are a straight line, every commit passed CI, and
   `main` stuck at Week 1 misrepresents the project. The week branches
   stay as markers.
2. **A GitHub repo?** Yes, **private**: `Luke458/retail-lakehouse`. CI
   needs a real repo to prove "a broken PR fails, a valid one merges".
   Private is the low-exposure choice; making it public, for the portfolio,
   is one click later, once the author has read it.
3. **Azure?** No. There is no subscription or `az` login on this machine,
   and spending money stays out of bounds. Week 5 stays code-only. The cloud
   run, screenshot and cost log remain the one open item of the plan's
   Phase 1.
4. **verity's materiality?** Keep verity's defaults. Materiality as a share
   of the compared history is verity's design. A 0.04% restatement
   publishing is acceptable, since small vendor restatements are normal and
   the decision records the revision (+2,058). Tuning it on the same weeks
   I measured would only make the numbers look better.
5. **Release the falsely blocked clean weeks?** Yes, each one only after
   checking that its silver equals the Favorita source, and with that
   evidence as the reason. verity's status stays on record next to each
   release, and the 9/20 false-alarm rate stands in `notes/week-03.md`.
   Gold should serve every week the data supports.
6. **VACUUM silver?** Yes, now, with a short retention. The old versions are
   artefacts of the pre-fix MERGE (each one the whole table again), nothing
   reads silver's history, and the data can be rebuilt from raw. Not the
   gate table: verity's stored assessments refer to its versions.
7. **Podman?** The author restarted it. Claude doesn't run `podman` at all:
   from this sandboxed shell it deletes podman's pid files and breaks the
   author's Podman (memory `never-run-podman-in-sandbox`). No container
   work in this loop.

## Guardrails (replacing the first loop's git and Podman rules)

- **Data:** public or synthetic only. No employer or supplier names.
- **Git and GitHub:**
  - Push only to the private `Luke458/retail-lakehouse`. Never make it
    public, never force-push, never rewrite pushed history.
  - Before the first push, check the tree for secrets and for anything
    under `data/` or `.airflow/`.
  - Changes reach `main` through pull requests once the repo exists, and
    a PR merges only when CI is green.
- **No spend, no cloud:** no Azure, no `tofu plan/apply` against real
  credentials. GitHub Actions minutes on the free plan are fine.
- **Shared machine:**
  - Never run `podman`; containers are checked with `ps` only.
  - Avoid ports 8000, 8080 and 8096.
  - At most 2 Spark JVMs.
  - `pkill -f` patterns must not match my own shell.
- **verity:** an installed library, from `main`. Don't edit retail-qc.
- **Proof:** real runs, real numbers, written down. Failures are reported.
- **Blocked by the permission system:** don't work around it. Note the
  exact command for the author.

## Tasks

- [ ] 1. Record these answers in `notes/decisions.md` ("Close-out"), and
  fast-forward `main` to `week-06`.
- [ ] 2. Release the falsely blocked weeks. For each blocked clean week
  (the 9 in `notes/week-03.md`, plus 2016-04-19's clean redelivery):
  - check silver against the Favorita source (rows and units);
  - release it, with that evidence as the reason;
  - clear `decide` so gold builds.

  Then check that gold serves every week in silver and equals silver
  (`gold_units_match_silver`).
- [ ] 3. VACUUM silver (sales, transactions) with Airflow idle, a 1-hour
  retention, and the duration check disabled for that call only. Record the
  disk use before and after, and check that silver still reads and
  reconciles (a rerun of one week is a no-op).
- [ ] 4. Create the private repo and push `main` and the week branches.
  Watch the CI workflow on GitHub and fix what differs from local runs
  until it is green.
- [ ] 5. The PR proof:
  - a PR with a deliberately broken change fails CI, then is closed;
  - a PR with a real change (the Week 4 notes and README, now that CI runs
    on GitHub) passes and is merged;
  - the deploy workflow runs on `main` and uploads the bundle.

  Record the run URLs and timings in `notes/week-04.md`.
- [ ] 6. Week 7 preparation, without writing the CV:
  - run career-ops `node upskill.mjs` and record what it says the project
    now covers;
  - write `notes/evidence.md`: every claim the project supports, each with
    its proof link. Factual and numbered, as source material for the
    author's CV v2. Update the career-plan status line.

## Log

- 2026-11-05: goal written. The first loop's questions answered above.
