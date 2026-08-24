# Golf Official Field Checker

The Hub's **Golf - Non-Runner Check** monitors the official PGA Tour, PGA Tour
Champions, DP World Tour, and LPGA field or entry-list pages. It replaces the
older Data Golf versus Betfair comparison.

## Operation

- The Hub starts the checker automatically, polls every five minutes, and keeps
  it running continuously until it is deliberately stopped.
- A Hub restart starts it again automatically.
- Once valid weekly URLs are available, startup sends a green Slack message
  listing every enabled official page. A graceful/manual stop sends a red
  scanner-offline message.
- The first valid read for a tournament is a silent baseline.
- PGA Tour, PGA Tour Champions, and DP World Tour changes must appear on two
  consecutive checks before Slack is notified.
- LPGA changes must appear on four consecutive checks because its virtualized
  page has historically produced inconsistent partial reads.
- Reserve-list reshuffling is ignored. LPGA is the exception: everyone found
  is counted as field because its field/reserve boundary is not reliable.
- A URL change is treated as a new tournament and creates a fresh silent
  baseline.
- A Slack failure does not commit the change to state, so delivery is retried.

## Weekly URLs

Open **Golf - Non-Runner Check** in the Hub. Save the official URLs and disable
any tour with no tournament that week.

- PGA Tour: tournament **Field** tab; URL contains `/tournaments/.../R.../field`.
- PGA Tour Champions: tournament **Field** tab under `/pgatour-champions/`;
  tournament identifiers normally begin with `S`.
- DP World Tour: tournament **Entry List** tab; URL ends with `/entry-list`.
- LPGA: tournament **Entries** tab; URL ends with `/entries`.

The checker reloads this configuration before every cycle, so saving URLs does
not require a process restart. Runtime configuration is stored in
`runtime/config/golf_field_checker.json` and is intentionally ignored by Git.

The Golf page also shows all four competitions with their current field and
reserve counts, tracking start time, latest successful check, and official
link. Its confirmed-change table records every addition and withdrawal for the
current tournament URL with a UK timestamp. Changing a tournament URL hides
the old tournament's history and starts a new history with the silent baseline.

## Slack and state

Slack continues to use `GOLF_NR_SLACK_WEBHOOK_URL`, or
`GOLF_NR_SLACK_BOT_TOKEN` with `GOLF_NR_SLACK_CHANNEL`. The legacy shared Slack
settings remain fallbacks for compatibility.

Per-tour state is stored under `runtime/output/golf_field_checker/`. If a site's
saved count is clearly wrong, stop the checker, remove only that tour's JSON
file, and start the checker again. The next valid read becomes a silent
baseline.

## User-triggered Betfair comparison

The **Check with Betfair** button runs independently of the continuous official
field scanner. It calls the Betfair Exchange API, finds the best matching active
Golf winner market for each enabled competition, and compares active Betfair
runners with the confirmed official field.

The Hub retains the latest result and shows matching, mismatch, baseline-not-
ready, event-not-matched, or API-error status per competition. A mismatch lists
each golfer as either official-field-only or Betfair-only and sends a fresh
message to the Golf Slack destination for every user-triggered run. Ambiguous
event matches and empty Betfair reads are shown as attention/errors and do not
send false discrepancy alerts.

## Site-reading safeguards

All four sites render their player lists with JavaScript, so Playwright and its
Chromium browser are required. PGA Tour and DP World Tour also reject the
default automated headless browser. On Ubuntu the scanner therefore starts a
private virtual display with `Xvfb` and runs Chromium in normal display mode;
install the `xvfb` system package before starting the Hub. A read below 60% of
the previous field size is rejected as likely incomplete. After six consecutive
short reads, the three non-LPGA sites accept a fresh silent baseline. LPGA never
auto-resets on short reads.

If LPGA remains chronically undercounted on EC2, inspect the Hub output first.
Its list is virtualized and has previously depended on visible browser rendering;
headless behavior may need revisiting if the official site changes.
