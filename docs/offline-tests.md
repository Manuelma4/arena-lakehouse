# Offline software verification

The default Python suite installs its service boundary before application tests
are collected. It removes inherited Arena, Ollama, MLflow, LangSmith and provider
configuration and disables `.env` loading. Calling the CLI in a fixture test
therefore cannot load credentials from the developer's checkout.

Real HTTPX transports, socket connections and DNS lookups are rejected before
transport. Provider tests keep their explicit mock transports and owned response
fixtures. Standard-library internal socket pairs remain available through a
narrow thread-local exception needed on Windows. Arbitrary localhost connections
are still rejected.

Rejected operations fail ordinary tests even when code catches the exception.
The JSON report separates collection, runtime and unattributed events, and the
exact boundary-test file identifies intentional rejections. Child pytest fixtures
exercise caught collection and late-session failures with unrelated plugin
autoload disabled; ordinary parent-suite plugin behavior is preserved.

The boundary tests use HTTPX directly, including a small in-process ASGI fixture;
they add no web-framework dependency to this data-engineering repository.
Spark-related unit tests use doubles or reject invalid configuration before
creating a Spark session. They do not establish native Spark, streaming restart,
million-observation performance or Databricks execution.

This is a Python test-process boundary, not an operating-system firewall or a
sandbox for arbitrary child programs. Provider acquisition and native-engine
verification are separate activities. No collector, provider dataset, production
pipeline, workflow or application asset is changed by this increment.

The source-evidence exporter is in a separate feature worktree. This test
increment starts from the approved regulation checkpoint and does not include
the exporter feature's tests in its default-suite result.
