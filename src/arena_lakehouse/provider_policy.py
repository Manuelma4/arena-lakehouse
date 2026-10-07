"""Provider names, budgets and sanitized error categories; no runtime imports."""

PROVIDERS = {
    "football-data": dict(token="FOOTBALL_DATA_TOKEN", host="api.football-data.org", limit=10, window=60),
    "pandascore": dict(token="PANDASCORE_TOKEN", host="api.pandascore.co", limit=1000, window=3600),
}
ERROR_CODES = frozenset({
    "awaiting_credentials", "quota_exhausted", "authentication_denied", "provider_rate_limited",
    "provider_unavailable", "provider_http_error", "network_error", "network_timeout",
    "invalid_provider_payload", "ingest_failed", "publication_failed", "cursor_commit_failed",
    "worker_interrupted", "clock_regression", "invalid_endpoint", "worker_error",
})
