"""Synthetic input corpus for the triage pipeline.

Deliberately generic SaaS support tickets (design doc §7.8). Nothing here is
domain-specific to any prior employer's product, taxonomy, or phrasing — the
categories are the five most boring things a support inbox receives.

The corpus is small and fixed on purpose: a latency/cost harness wants the same
inputs every run so a change in the numbers means a change in the *system*, not
a change in the workload.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

# Category labels the classify node is allowed to emit. Kept here rather than in
# graph.py so the corpus and the classifier's vocabulary cannot drift apart.
CATEGORIES: tuple[str, ...] = (
    "billing_question",
    "login_problem",
    "feature_request",
    "bug_report",
    "refund_request",
)

SEVERITIES: tuple[str, ...] = ("low", "medium", "high")

ACTIONS: tuple[str, ...] = (
    "auto_resolve",
    "route_to_human",
    "request_more_info",
    "escalate",
)


@dataclass(frozen=True)
class Ticket:
    ticket_id: str
    subject: str
    body: str
    channel: str  # web_form | email | in_app
    plan: str  # free | pro | enterprise
    # The category a human would assign. Never shown to the model — it exists so
    # the simulated provider can return a plausible answer instead of noise.
    topic: str


TICKETS: tuple[Ticket, ...] = (
    Ticket(
        ticket_id="TCK-1001",
        subject="Charged twice this month",
        body=(
            "My card shows two charges for the same billing period. "
            "One on the 3rd and one on the 4th, same amount. Which one is real?"
        ),
        channel="email",
        plan="pro",
        topic="billing_question",
    ),
    Ticket(
        ticket_id="TCK-1002",
        subject="Can't sign in after password reset",
        body=(
            "I reset my password from the email link and now the login page just "
            "reloads without an error. Tried two browsers and an incognito window."
        ),
        channel="web_form",
        plan="free",
        topic="login_problem",
    ),
    Ticket(
        ticket_id="TCK-1003",
        subject="Export to CSV please",
        body=(
            "Would love a way to export the table view to CSV. Copy-pasting into a "
            "spreadsheet loses the column types every time."
        ),
        channel="in_app",
        plan="pro",
        topic="feature_request",
    ),
    Ticket(
        ticket_id="TCK-1004",
        subject="Dashboard shows blank chart",
        body=(
            "Since this morning the usage chart renders an empty box. Console shows "
            "a 500 from /api/usage. Other pages load fine. Started around 09:15 UTC."
        ),
        channel="in_app",
        plan="enterprise",
        topic="bug_report",
    ),
    Ticket(
        ticket_id="TCK-1005",
        subject="Refund for annual plan",
        body=(
            "I upgraded to annual by mistake three days ago and meant to stay "
            "monthly. Can I get the difference refunded?"
        ),
        channel="email",
        plan="pro",
        topic="refund_request",
    ),
    Ticket(
        ticket_id="TCK-1006",
        subject="Invoice missing tax ID",
        body=(
            "Our finance team rejected the last invoice because it has no VAT "
            "number on it. Where do I add the company tax ID?"
        ),
        channel="email",
        plan="enterprise",
        topic="billing_question",
    ),
    Ticket(
        ticket_id="TCK-1007",
        subject="SSO loop for the whole team",
        body=(
            "Everyone on our workspace is bouncing between the SSO provider and "
            "your login screen. Nobody can get in. This is blocking eleven people."
        ),
        channel="email",
        plan="enterprise",
        topic="login_problem",
    ),
    Ticket(
        ticket_id="TCK-1008",
        subject="Dark mode",
        body="Any plans for a dark theme? The white background is rough at night.",
        channel="in_app",
        plan="free",
        topic="feature_request",
    ),
    Ticket(
        ticket_id="TCK-1009",
        subject="Uploads fail over 10MB",
        body=(
            "Attachments larger than about 10MB fail with a generic error after "
            "roughly thirty seconds. Smaller files upload fine. Reproducible."
        ),
        channel="web_form",
        plan="pro",
        topic="bug_report",
    ),
    Ticket(
        ticket_id="TCK-1010",
        subject="Cancel and refund unused seats",
        body=(
            "We downsized from 20 seats to 8 last week. Can the 12 unused seats be "
            "refunded for the remainder of the term?"
        ),
        channel="email",
        plan="enterprise",
        topic="refund_request",
    ),
    Ticket(
        ticket_id="TCK-1011",
        subject="What counts as an active user?",
        body=(
            "The invoice line says 'active users' but I can't find the definition "
            "anywhere. Does an API-only integration count?"
        ),
        channel="web_form",
        plan="pro",
        topic="billing_question",
    ),
    Ticket(
        ticket_id="TCK-1012",
        subject="Webhook retries hammering our endpoint",
        body=(
            "We returned a 503 for two minutes during a deploy and got about four "
            "thousand retries. Backoff does not appear to be applied."
        ),
        channel="in_app",
        plan="enterprise",
        topic="bug_report",
    ),
)


def ticket_for_run(run_index: int, rng: random.Random | None = None) -> Ticket:
    """Pick the ticket for a given run.

    Round-robin rather than random so a run of N covers the corpus evenly — a
    random draw over 20 runs leaves whole categories unvisited often enough to
    make the path distribution look broken when it isn't.
    """
    if rng is not None and run_index >= len(TICKETS):
        # Past one full pass, jitter the order so long runs don't produce a
        # perfectly periodic workload (which makes latency charts look fake).
        return rng.choice(TICKETS)
    return TICKETS[run_index % len(TICKETS)]


def render_ticket(ticket: Ticket) -> str:
    return f"Subject: {ticket.subject}\nChannel: {ticket.channel}\n\n{ticket.body}"
