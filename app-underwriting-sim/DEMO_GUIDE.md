# Red Bricks Insurance — Underwriting Portal: A Guided Tour

Welcome! This is a friendly, plain-language walkthrough of the Underwriting Simulation Portal — what each screen is for and how you'd actually use it day to day. No technical background needed. Think of it as a tour guide, not a manual.

**What the app is, in one sentence:** a single workbench where underwriters, actuaries, account executives, and finance leaders can intake a group, price it every which way, pressure-test the decision, route it for approval, and track it from quote to sold — all on live book-of-business numbers.

---

## First things first: the "Viewing as" bar

Across the very top of every screen you'll see a row of chips: **All Capabilities · CFO / Chief Actuary · Underwriter · AE / Broker · Actuarial Team.**

Pick the one that matches your role and the left-hand menu instantly trims down to just the screens that matter to you. An AE sees the client-facing tools; an actuary sees the factor and risk screens; a CFO sees portfolio, approvals, and governance. Nothing is deleted — switch back to **All Capabilities** any time to see everything. It's the fastest way to make a demo feel tailored to whoever is in the room.

---

## The core workflow screens

### Dashboard — your book at a glance
**What it's for:** the home base. The moment you log in you see where the business stands.

**How you'd use it:** glance at the top cards — total premium, total claims, overall medical loss ratio (MLR), member count. Below that, a by-line-of-business table shows how Commercial, Medicare Advantage, and Medicaid are each performing, with anything running hot flagged in red. The "Quick Simulate" tiles let you jump straight into a what-if, and "Recent Simulations" picks up whatever you were last working on.

**Why it matters:** it answers "how are we doing?" before anyone asks.

### Quote Intake — turn a messy submission into a clean quote
**What it's for:** the front door for new business and renewals. Instead of re-keying a broker's email or a census spreadsheet, you paste it in and the app structures it for you.

**How you'd use it:** choose **NL Submission** (paste a plain-language note from an AE) or **Document Extract** (paste a census, benefit summary, or competitor quote). Click **Parse** and the app fills in the group name, size, line of business, current premium and claims, effective date, and more. You then get a **completeness gauge** — "pre-filled 78%; still missing: effective date, funding type" — and a short **strategy memo** suggesting what to validate first. There's a "Load sample" button so a demo always has something to try.

**Why it matters:** the AE *verifies* a pre-filled submission instead of starting from a blank page.

### Packaged Scenarios — four ready-made renewal options
**What it's for:** generating the classic renewal choices side by side from one set of inputs.

**How you'd use it:** enter the group (or leave blank to use book averages), your target MLR, admin load, expected market trend, and optionally a competitor's rate. Click **Generate Scenarios** and you get four cards — **Standard** (hits your margin target), **Competitive** (wins the deal), **Retention** (keeps the group), and **Custom** (your own number) — each showing the rate change, projected MLR, margin, and the odds the group renews. The app then **recommends** the one that makes the most money *after* accounting for the chance the group walks, and explains why in plain English.

**Why it matters:** it turns "what should we quote?" into a one-click, defensible answer.

### Funding Quote — price it the way the group wants to buy
**What it's for:** pricing a group under whatever funding arrangement fits — fully insured, self-funded (ASO), level-funded, specialty (dental/vision), stop-loss, or disability.

**How you'd use it:** pick the arrangement at the top, fill in the group details and a few arrangement-specific knobs, and click **Price Quote**. You get a clean cost build-up (claims, admin, stop-loss, margin — whatever applies), the total annual cost and PMPM, who carries the risk, and how it compares to a fully-insured baseline. From there you can **Save** the quote (it enters the pipeline) or **Route for Approval** in one click.

**Why it matters:** one screen handles every way a group can be funded, with the right math behind each.

### Simulation Builder — the deep what-if lab
**What it's for:** modeling a single lever in detail. Eleven scenario types, from premium rate changes to medical trend, stop-loss thresholds, risk adjustment, and IBNR reserves.

**How you'd use it:** pick a scenario type, fill in the form, and click **Run**. You immediately see baseline vs. projected numbers with the changes color-coded, a plain-language narrative of what happened, and warnings if something crosses a line (e.g., "projected MLR exceeds the rebate threshold"). Save it to revisit or compare later.

**Why it matters:** when someone asks "what if we…?", this answers it in under a second.

---

## Actuarial screens

### Rate Build-Up — show your work
**What it's for:** the transparent, step-by-step construction of a community-rated premium.

**How you'd use it:** enter the group's age band, geography, industry, experience, and trend. The app walks you down the waterfall — base rate → age → area → industry → experience → trend → final PMPM — showing the factor applied at each step. A badge tells you whether the factors came from the governed, actuary-approved table or the built-in defaults.

**Why it matters:** no black box. Every dollar of the rate is explainable to a regulator, a broker, or a group.

### Risk Pool Analysis — how healthy is this group?
**What it's for:** comparing a group's risk profile against the whole book.

**How you'd use it:** enter a group ID and the app charts its risk-score and age distributions, chronic-condition prevalence, and top cost drivers against the book average — and flags **adverse selection** if the group skews sicker than it's priced for, with a severity level and a recommendation.

**Why it matters:** it catches a group that looks fine on premium but is quietly a loss waiting to happen.

### Factor Governance — actuaries own the dials
**What it's for:** reviewing, approving, and publishing the rating factors the whole app uses.

**How you'd use it:** snapshot the current factors into a new draft version, add a note ("Q4 trend refresh"), then walk it through **Draft → Approve → Publish**. Publishing pushes the approved factors live so every rate build-up thereafter uses them. The full version history, with who did what and when, stays on the page.

**Why it matters:** the system computes, but *actuaries govern* — and there's a clean trail proving it.

---

## Management screens

### Compare Scenarios — put options head to head
**What it's for:** a side-by-side of two to four saved simulations.

**How you'd use it:** pick the scenarios you saved earlier and view them in one comparison so a committee can see the trade-offs at a glance, with notes attached.

### Quote Pipeline — from rated to sold to implemented
**What it's for:** the lifecycle tracker and operational dashboard in one.

**How you'd use it:** at the top, operational KPIs — win rate, approval rate, average decision time, and how much the rating factors have drifted. Below, every saved quote with its stage; advance a quote **Rated → Sold → Implemented** (or mark it Lost) as the deal moves. Need to tweak a live quote? Click **Re-rate**, type the change in plain language — "raise the specific attachment to $300K" or "+150 lives" — and the app re-prices it and keeps the revision history.

**Why it matters:** it's the audit trail of what you quoted vs. what actually sold — something most teams have never had.

### Approvals — the right people sign off
**What it's for:** routing decisions to the correct authority level and recording the outcome.

**How you'd use it:** the top shows the **authority matrix** — which approver tier can sign off on which dollar and rate-change size. Below is the live queue: each pending item shows who it routed to and why, and an approver clicks **Approve**, **Needs Info**, or **Reject**. Every action is logged.

**Why it matters:** no more "who was allowed to approve this?" — the rules are visible and enforced.

### Simulation History — your saved work
**What it's for:** the running list of everything you've saved.

**How you'd use it:** filter and open any past simulation to see its full detail, move it through its status (draft → computed → approved → archived), add notes, and view its audit trail — attributed to the real person who made each change.

---

## Assistants

### Underwriting Agent — ask in plain English
**What it's for:** a conversational helper that can run simulations and look up data for you.

**How you'd use it:** type a question — "price a 300-life Commercial renewal at a 5% increase and tell me the MLR impact" — and watch it work: it narrates each step, runs the real engine, and comes back with a quantitative answer. It explains and calculates; it never invents numbers.

### Genie Search — natural-language data questions
**What it's for:** asking the underwriting data a question without writing a query.

**How you'd use it:** type something like "average PMPM by line of business last year," and Genie returns the answer as a table along with the query it used, so you can trust where the number came from.

---

## Trust & oversight

### Digital Twin — a second opinion before you commit
**What it's for:** an advisory pre-implementation check. Before you lock in a rating or underwriting policy, the twin pressure-tests it against the organization's accumulated experience.

**How you'd use it:** describe the decision you're about to make — arrangement, group size, scenario, the rate change, projected margin and MLR, and your reasoning. Click **Run Pre-Implementation Check** and you get back a clear **GREEN / AMBER / RED** verdict, the dollars at stake, which guardrails it tripped (if any), the approval tier it needs, and — most importantly — a grounded critique that **cites similar past decisions and how they actually turned out** ("you priced a similar 3,000-life manufacturer this aggressively last quarter; it ran 94% MLR and lost $1.8M"). Underneath, you can see the institutional memory it's drawing on and the active guardrails.

**Why it matters:** on million-dollar decisions it checks you, learns your tendencies over time, and surfaces the dissent a confident analyst might miss — while leaving the final call to the human.

### Governance — the trust story on one page
**What it's for:** the model-risk and compliance posture of the whole app.

**How you'd use it:** show a regulator or an internal risk team the factor provenance (governed vs. fallback), the inventory of every model the app uses and how a human oversees each, the explainability basis for every rate and recommendation, and the consumer-protection posture. It's the "we can defend this" screen.

### Observability — is it healthy and what does it cost?
**What it's for:** keeping an eye on the AI assistants.

**How you'd use it:** review recent activity traces and the token usage and estimated cost per model, so there are no surprises on performance or spend.

---

## A typical end-to-end flow (how it all connects)

1. A broker submission lands → **Quote Intake** structures it and flags what's missing.
2. **Packaged Scenarios** generates the renewal options and recommends one; **Funding Quote** prices the arrangement the group wants.
3. **Rate Build-Up** and **Risk Pool Analysis** let the actuary sanity-check the pricing and the group's health.
4. Before committing an aggressive call, the **Digital Twin** gives a GREEN/AMBER/RED second opinion grounded in past decisions.
5. The quote routes through **Approvals**, then rides the **Quote Pipeline** from Rated → Sold → Implemented, with re-rates captured along the way.
6. **Governance** and **Observability** keep the whole thing auditable and healthy, while **Factor Governance** keeps the actuaries in control of the dials.

That's the tour. Switch your persona at the top, start on the Dashboard, and follow a group all the way through — the app is built to tell that story end to end.
