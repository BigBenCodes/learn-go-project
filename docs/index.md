# Go Fraud Pipeline

A deliberately small, production-shaped fraud-scoring pipeline for learning backend Go: concurrency, Kafka partitioning, idempotency, database transactions, the outbox pattern, cancellation, retries, HTTP APIs, structured logs, and metrics.

It generates mock card transactions, scores them asynchronously with a transparent (hand-authored, untrained) logistic-regression model, recommends an action, and later evaluates those predictions against delayed ground-truth labels.

!!! note "This is a learning system"
    Synthetic data, hand-authored model weights, a single local broker, no authentication, no real payment intervention. See [Design Decisions and Testability](explanation/design-decisions-and-testability.md) for the full scope statement.

## Find what you need

<div class="grid cards" markdown>

-   :material-rocket-launch:{ .lg .middle } **Tutorials**

    ---

    New here? Run the whole pipeline end to end in one guided walkthrough.

    [:octicons-arrow-right-24: Getting Started](tutorials/getting-started.md)

-   :material-hammer-wrench:{ .lg .middle } **How-To Guides**

    ---

    Goal-oriented recipes: reproduce a run, reset infrastructure, run tests, try the suggested experiments.

    [:octicons-arrow-right-24: Browse guides](how-to/reproduce-simulator-runs.md)

-   :material-book-open-variant:{ .lg .middle } **Reference**

    ---

    HTTP API, CLI/Make commands, and configuration — dry, structured lookup tables.

    [:octicons-arrow-right-24: HTTP API](reference/http-api.md)

-   :material-lightbulb-on:{ .lg .middle } **Explanation**

    ---

    Architecture, delivery guarantees, scoring, and the design decisions behind them.

    [:octicons-arrow-right-24: Architecture Overview](explanation/architecture-overview.md)

</div>

## Source

This site documents [github.com/BigBenCodes/learn-go-project](https://github.com/BigBenCodes/learn-go-project). Every page is grounded in the repository's own code and README — use "Edit this page" in the header to see or fix the source Markdown.
