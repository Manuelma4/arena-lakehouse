"""SQL contract expressions for the optional managed Spark adapter.

These row expectations complement the canonical upstream validator. They do not
replace its cross-revision identity/time checks or its conflict ledger.
"""

QUALITY_RULES = {
    "known_shape": "_rescued_data IS NULL",
    "identifiers": "event_id IS NOT NULL AND length(event_id) BETWEEN 1 AND 180 AND "
                   "match_id IS NOT NULL AND length(match_id) BETWEEN 1 AND 120",
    "revision": "revision IS NOT NULL AND revision >= 1",
    "source": "provider IN ('synthetic','football-data','pandascore') AND sport IN ('football','valorant')",
    "teams": "team_a_id IS NOT NULL AND team_b_id IS NOT NULL AND length(team_a_id)>0 AND length(team_b_id)>0 "
             "AND team_a_id <> team_b_id AND length(team_a) BETWEEN 1 AND 100 AND length(team_b) BETWEEN 1 AND 100",
    "competition": "length(competition) BETWEEN 1 AND 120",
    "status": "status IN ('scheduled','running','finished','cancelled','postponed')",
    "utc_times": "try_to_timestamp(start_time) IS NOT NULL AND try_to_timestamp(observed_at) IS NOT NULL "
                 "AND start_time RLIKE '(Z|[+-][0-9]{2}:[0-9]{2})$' "
                 "AND observed_at RLIKE '(Z|[+-][0-9]{2}:[0-9]{2})$'",
    "scores": "(score_a IS NULL OR score_a BETWEEN 0 AND 50) AND (score_b IS NULL OR score_b BETWEEN 0 AND 50)",
    "best_of": "best_of BETWEEN 1 AND 9",
    "finished": "(status='finished' AND score_a IS NOT NULL AND score_b IS NOT NULL AND completed_at IS NOT NULL "
                "AND completed_at RLIKE '(Z|[+-][0-9]{2}:[0-9]{2})$' "
                "AND try_to_timestamp(start_time)<try_to_timestamp(completed_at) "
                "AND try_to_timestamp(completed_at)<=try_to_timestamp(observed_at)) "
                "OR (status<>'finished' AND completed_at IS NULL)",
    "series": "sport<>'valorant' OR status<>'finished' OR (best_of % 2=1 AND score_a<>score_b "
              "AND greatest(score_a,score_b)=floor(best_of/2)+1 "
              "AND least(score_a,score_b)<floor(best_of/2)+1)",
}


def all_quality_rules():
    # COALESCE rejects three-valued SQL NULL instead of silently admitting it.
    return " AND ".join(f"coalesce(({expression}), false)" for expression in QUALITY_RULES.values())
