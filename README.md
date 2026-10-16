# jobs

A durable background job queue for SSPUR programs, stored in SQLite: the database you already
have, or a file of its own. It covers what BullMQ, Sidekiq, Oban and river do for a single
database: priorities, delays and run-at times, unique jobs, bulk enqueue, leases with a visibility
timeout so a crashed worker's jobs come back, heartbeats, retries with exponential, linear, fixed or
listed backoff (with repeatable jitter), a dead letter state, cancel, retry, snooze, purge, stats,
and cron schedules with time zones and a policy for missed runs.

Every operation takes the time as a `now: Time` argument, so tests are deterministic and a crash
can be simulated by moving the clock; `enqueue_now`, `work`, `drain` and `run_worker` read the
clock for production use.

## Install

```console
$ sspur add jobs
```

## Example

```sspur
use jobs.{Job, Outcome, Done, Retry}

type Email = {to: Str, subject: Str}

// The handler: a top-level function, so worker tasks can call it by name.
fn send_email(j: Job) -> Outcome
= match json.decode[Email](j.payload)
  | ok(e) => if e.to.contains("@") then Done else Retry{error: "no @ in {e.to}"}
  | err(e) => jobs.Discard{error: e}

fn main() -> Unit ! db, fail[DbErr], log, time, conc
= do
  c = db.open(":memory:")
  applied = jobs.migrate(c)
  t = now()
  welcome = jobs.job("mail", "welcome", json.encode(Email{to: "ada@example.com", subject: "Hi"}))
  a = jobs.enqueue(c, welcome with unique_key := some("welcome:ada"), t)
  again = jobs.enqueue(c, welcome with unique_key := some("welcome:ada"), t)
  b = jobs.enqueue(c, jobs.job("mail", "welcome", json.encode(Email{to: "nobody", subject: "Hi"})) with max_attempts := 2, t)
  bad = jobs.enqueue(c, jobs.job("mail", "welcome", "not json"), t)
  log("{applied} migrations; enqueued {a}, then {again}")
  w = jobs.worker("mail") with backoff := jobs.Fixed{delay: millis(0)}
  for i in par(0..2)
    n = jobs.drain(c, w, send_email, 100)
  for s in jobs.stats(c, now())
    log("{s.queue}: {s.completed} completed, {s.dead} dead, {s.available} waiting")
  for j in jobs.list(c, jobs.filter() with state := some(jobs.Dead))
    log("dead job {j.id} after {j.attempts} attempts: {j.last_error.or("")}")
  from = datetime(2026, 10, 16, 12, 0, 0).or(t)
  match jobs.parse_cron("0 9 * * MON-FRI")
  | ok(cron) => log("next weekday 09:00 UTC runs: {jobs.next_fires(cron, from, 3)}")
  | err(e) => log(e)
  log("bad expression: {jobs.parse_cron("0 25 * * *")}")
  p = jobs.put_schedule(c, jobs.schedule("digest", "*/5 * * * *", "mail", "digest", "\{}") with missed := jobs.CatchUp{limit: 10}, from)
  fired = jobs.tick(c, from.add(mins(12)))
  log("tick enqueued {fired.map(f => f.at)}")
```

Run as a dependent (`jobs = { path = "..." }`), natively and with `--interp`, the output is the
same:

```console
$ sspur run main.ssp
2 migrations; enqueued Inserted{id: 1}, then Duplicate{id: 1}
mail: 1 completed, 2 dead, 0 waiting
dead job 2 after 2 attempts: no @ in nobody
dead job 3 after 1 attempts: invalid JSON
next weekday 09:00 UTC runs: [2026-10-19T09:00:00Z, 2026-10-20T09:00:00Z, 2026-10-21T09:00:00Z]
bad expression: err("hour: 25 is out of range 0-23")
tick enqueued [2026-10-16T12:05:00Z, 2026-10-16T12:10:00Z]
```

The second job fails twice and goes to the dead letter state with its error; the third has a
payload that doesn't decode, so the handler discards it at once. The schedule was registered at
12:00 and ticked at 12:12, so with `CatchUp` it enqueues both runs it missed.

## How it works

Jobs live in `jobs_jobs` and schedules in `jobs_schedules`; `migrate(c)` creates them and records
its own versions in `jobs_schema`, so it never interferes with `db.migrate`. Times are stored as
Unix milliseconds.

A job is `Pending` (waiting for its `run_at`; a retry is a pending job with attempts made),
`Running` (leased to a worker) or final: `Completed`, `Dead` or `Cancelled`.

`claim` runs one `BEGIN IMMEDIATE` transaction that first rescues expired leases of the queue, then
takes the best available job (smallest `priority`, then earliest `run_at`, then oldest) and leases
it until `now + lease`. Each claim increments the attempt count and the lease `token`. Every
finishing call (`complete`, `fail_attempt`, `retry_in`, `discard`, `snooze`, `extend`) checks the
token, so a worker whose lease expired and whose job another worker took over can't record a
result: it gets `false`. A completion after the lease expired is still accepted as long as nobody
claimed the job again. A rescued job counts the lost attempt, and goes to `Dead` if that was its
last.

Because SQLite serializes writers, claims from any number of threads and processes never hand out
one job twice; the tests run 6 concurrent workers over 300 jobs on a file database and check that
every job ran exactly once.

### Workers and concurrency

A handler is a function `Job -> Outcome`. It decides what happens: `Done`, `Retry{error}` (by the
worker's backoff), `RetryIn{error, after}`, `Discard{error}` (dead now) or `Snooze{after}` (later,
without counting the attempt). A handler that traps or hangs leaves its job leased; the lease
expiring brings it back.

`work` processes one job, `drain` until the queue is empty or a maximum, and `run_worker` until an
`Atomic[Int]` stop flag is set, sleeping between polls. SSPUR tasks can't share function values, so
to run several workers at once start them in `par` and name a top-level handler (or write the lambda
inside the task):

```sspur
for i in par(0..4)
  n = jobs.drain(c, w, send_email, 1000)
```

The step API (`claim`, `claim_many`, `extend`, `finish` and the single outcomes) is there for
callers that drive the loop themselves, for example to batch work.

### Unique jobs

With `unique_key`, enqueueing again into the same queue returns `Duplicate{id}` of the existing job
while it is pending or running, and also, when `unique_for` is set, until that long after the first
enqueue (whatever its state). The check and the insert are one transaction.

### Cron and schedules

`parse_cron` reads 5-field expressions (minute, hour, day of month, month, day of week), 6-field
ones with seconds first, and the macros `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`,
`@midnight` and `@hourly`. Fields take `*`, numbers, ranges, steps (`*/15`, `1-30/5`, and `5/15`
meaning `5-59/15`), lists and names (`JAN`-`DEC`, `SUN`-`SAT`, any case); 0 and 7 are Sunday.
Errors name the field: `hour: 25 is out of range 0-23`, `minute: step '0' must be a positive
number`, `day of month: 'L' is not supported (it is a Quartz extension)`.

The two day fields follow Vixie cron exactly: when both are restricted, a day matches if either
does (`0 0 1 * MON` is the 1st and every Monday); when either starts with `*`, both must match. That
includes the well-known surprise that `*/2` in the day of month counts as `*` for this rule, so
`0 0 */2 * MON` means odd-numbered Mondays; write `1-31/2` for "odd days or Mondays".

`next_fire`, `prev_fire` and `next_fires` work in UTC; the `_in` variants take a `Zone`
(`time_zone("Europe/Berlin")`). On the wall clock, a time the clock skips at the start of daylight
saving time is shifted forward by the gap (02:30 becomes 03:30), and a time that happens twice when
it ends fires once, at its first occurrence. Searches look up to nine years ahead, so an expression
that can never fire (`0 0 30 2 *`) gives `none` rather than looping.

`put_schedule` stores a schedule (validating its expression and zone) and `tick(c, now)` enqueues
every due run once: each run's unique key is `schedule:NAME:MILLIS` and the schedule's progress
moves in the same transaction, so ticking twice or from several processes is safe. Runs missed while
nothing ticked follow the schedule's `missed`: `CatchUp{limit}` (each, up to the most recent
`limit`), `Latest` (only the most recent) or `Skip{grace}` (the most recent if it is at most
`grace` old).

## API

### Queue setup

| Definition | What it is |
|---|---|
| `migrate(c)` | Creates or upgrades the package's tables; safe on every start |
| `State` | `Pending`, `Running`, `Completed`, `Dead`, `Cancelled` |
| `Job`, `AttemptError` | A stored job, with the history of its failed attempts (the last 25) |

### Enqueueing

| Definition | What it is |
|---|---|
| `NewJob`, `job(queue, kind, payload)` | A job to add, with defaults: priority 0, now, 20 attempts, not unique |
| `enqueue(c, j, now)`, `enqueue_now(c, j)` | Adds a job, or finds the one with its unique key |
| `enqueue_all(c, js, now)` | Adds many in one transaction |
| `Enqueued`, `enqueued_id(e)` | `Inserted{id}` or `Duplicate{id}` |

### Claiming and finishing

| Definition | What it is |
|---|---|
| `Lease` | A claimed job with its token and expiry |
| `claim(c, queue, lease, now)`, `claim_many(c, queue, n, lease, now)` | Lease the next available jobs |
| `extend(c, l, by, now)` | Heartbeat: push the lease out |
| `complete`, `fail_attempt`, `retry_in`, `discard`, `snooze` | Record one outcome; false when the lease was lost |
| `Outcome`, `finish(c, l, o, backoff, now)` | A handler's decision, and recording it |
| `rescue(c, now)` | Bring back jobs whose lease expired, in every queue |

### Backoff

| Definition | What it is |
|---|---|
| `Backoff` | `Exponential{base, cap, jitter}`, `Fixed{delay}`, `Linear{step, cap}`, `Steps{delays}` |
| `backoff_delay(b, attempt, seed)` | The wait after `attempt` attempts; repeatable for a seed |
| `default_backoff()` | Exponential from 1 s to 1 h with 25% jitter |

### Workers

| Definition | What it is |
|---|---|
| `Worker`, `worker(queue)` | Queue, lease length (5 minutes) and backoff |
| `Ran` | `Idle` or `Processed{job, outcome, recorded}` |
| `work_at(c, w, handler, now)`, `work(c, w, handler)` | Process one job |
| `drain(c, w, handler, max)` | Process until empty or `max` |
| `run_worker(c, w, handler, stop, idle_ms)` | Serve the queue until `stop` is set |

### Managing and inspecting

| Definition | What it is |
|---|---|
| `get(c, id)` | One job |
| `Filter`, `filter()`, `list(c, f)` | Jobs by queue, state and kind, with keyset paging |
| `QueueStats`, `stats(c, now)` | Counts per queue and state, and the oldest waiting job |
| `cancel(c, id, now)` | Cancel a pending or running job |
| `retry(c, id, now)`, `retry_dead(c, queue, now)` | Bring dead or cancelled jobs back |
| `reschedule(c, id, at)` | Move a pending job |
| `delete(c, id)`, `purge(c, state, before)` | Remove jobs |

### Cron

| Definition | What it is |
|---|---|
| `Cron`, `parse_cron(expr)` | A parsed expression, or an error naming the field |
| `cron_matches(c, t)`, `cron_matches_in(c, z, t)` | Whether a time is a fire time |
| `next_fire`, `prev_fire`, `next_fires` | Fire times in UTC |
| `next_fire_in`, `prev_fire_in`, `next_fires_in` | Fire times on a zone's wall clock |

### Schedules

| Definition | What it is |
|---|---|
| `Schedule`, `schedule(name, cron, queue, kind, payload)` | A recurring job, in UTC by default |
| `Missed` | `CatchUp{limit}`, `Latest`, `Skip{grace}` |
| `put_schedule(c, s, now)`, `remove_schedule(c, name)`, `set_enabled(c, name, on, now)`, `schedules(c)` | Manage them |
| `due_fires(cron, zone, last, now, missed)` | The runs a tick would enqueue (pure) |
| `Fired`, `tick(c, now)` | Enqueue every due run once |

## Out of scope

- Other stores (Redis, Postgres): the queue relies on SQLite's single writer for its guarantees.
- Job dependencies, flows and batches with callbacks; rate limiting per queue (use a rate limiter
  in the handler and `Snooze`).
- Pushing results to waiting callers: store results yourself, keyed by job id.
- Killing a running handler: SSPUR has no way to stop a task from outside, so timeouts are leases.
- Quartz cron extensions (`L`, `W`, `#`, `?`) and `@reboot`.

## License

MIT, see [LICENSE](LICENSE).
