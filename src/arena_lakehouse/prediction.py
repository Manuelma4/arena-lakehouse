"""Point-in-time team-form baseline; outcome labels never enter feature calculation."""

import math
from collections import defaultdict
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, log_loss
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from .engine import latest
from .schema import outcome

FEATURE_NAMES = ["elo_difference", "recent_form_difference", "team_a_history", "team_b_history"]


def features(target: dict, events: list[dict], cutoff: str) -> tuple[list[float], dict]:
    known = latest(events, cutoff)
    history = [m for m in known if m["sport"] == target["sport"] and m["status"] == "finished"
               and m["match_id"] != target["match_id"] and m["completed_at"] < target["start_time"]]
    history.sort(key=lambda x: (x["completed_at"], x["match_id"]))
    ratings, form = defaultdict(lambda: 1500.0), defaultdict(list)
    for match in history:
        a, b = match["team_a_id"], match["team_b_id"]
        actual = {"a": 1.0, "draw": 0.5, "b": 0.0}[outcome(match)]
        expected = 1 / (1 + 10 ** ((ratings[b] - ratings[a]) / 400))
        delta = 24 * (actual - expected)
        ratings[a] += delta
        ratings[b] -= delta
        form[a].append(actual)
        form[b].append(1 - actual)
    a, b = target["team_a_id"], target["team_b_id"]
    recent_a = float(np.mean(form[a][-5:])) if form[a] else 0.5
    recent_b = float(np.mean(form[b][-5:])) if form[b] else 0.5
    values = [(ratings[a] - ratings[b]) / 400, recent_a - recent_b,
              min(len(form[a]), 30) / 30, min(len(form[b]), 30) / 30]
    evidence = dict(elo_a=round(ratings[a], 2), elo_b=round(ratings[b], 2),
                    recent_form_a=recent_a, recent_form_b=recent_b,
                    historical_matches_a=len(form[a]), historical_matches_b=len(form[b]),
                    feature_cutoff=cutoff, latest_result_used=max(
                        (m["observed_at"] for m in history), default=None))
    return values, evidence


def score(y, probs, classes):
    onehot = np.array([[float(label == c) for c in classes] for label in y])
    return dict(samples=len(y), accuracy=float(accuracy_score(y, np.array(classes)[probs.argmax(axis=1)])),
                log_loss=float(log_loss(y, probs, labels=classes)),
                multiclass_brier=float(np.mean(np.sum((probs - onehot) ** 2, axis=1))))


def train_and_predict(events: list[dict]) -> tuple[list[dict], dict]:
    current = latest(events)
    kinds = {e["provider"] for e in events}
    if "synthetic" in kinds and len(kinds) != 1:
        raise ValueError("Synthetic and real data must not train or evaluate the same model")
    data_kind = "synthetic" if kinds == {"synthetic"} else "api"
    predictions, reports = [], {}
    available_as_of = max((e["observed_at"] for e in events), default=None)
    for sport in ("football", "valorant"):
        classes = ["a", "b", "draw"] if sport == "football" else ["a", "b"]
        completed = sorted([m for m in current if m["sport"] == sport and m["status"] == "finished"],
                           key=lambda x: (x["start_time"], x["match_id"]))
        # Results collected today cannot masquerade as observations before historical kickoffs.
        rows = []
        for match in completed:
            values, evidence = features(match, events, match["start_time"])
            if min(evidence["historical_matches_a"], evidence["historical_matches_b"]) >= 3:
                rows.append((match, values, outcome(match)))
        if len(rows) < 40:
            reports[sport] = dict(status="insufficient_point_in_time_history", eligible_matches=len(rows),
                                 data_kind=data_kind)
            continue
        train_end, validation_end = int(len(rows) * 0.6), int(len(rows) * 0.8)
        # All matches at a timestamp stay in the same partition.
        train_time, validation_time = rows[train_end][0]["start_time"], rows[validation_end][0]["start_time"]
        available_labels = {m["match_id"]: m for m in latest(events, train_time)
                            if m["status"] == "finished" and m["completed_at"] < train_time}
        train = [(r[0], r[1], outcome(available_labels[r[0]["match_id"]])) for r in rows
                 if r[0]["start_time"] < train_time and r[0]["match_id"] in available_labels]
        validation = [r for r in rows if train_time <= r[0]["start_time"] < validation_time]
        test = [r for r in rows if r[0]["start_time"] >= validation_time]
        if not train or not validation or not test or set(r[2] for r in train) != set(classes):
            reports[sport] = dict(status="insufficient_class_coverage", data_kind=data_kind)
            continue
        model = make_pipeline(StandardScaler(), LogisticRegression(C=1.0, max_iter=500, random_state=42))
        model.fit([r[1] for r in train], [r[2] for r in train])
        counts = np.array([sum(r[2] == c for r in train) for c in classes], dtype=float) + 1
        prior = counts / counts.sum()
        report = dict(status="evaluated", data_kind=data_kind, model="scaled_logistic_v1",
                      feature_names=FEATURE_NAMES, train_samples=len(train),
                      train_last_kickoff=train[-1][0]["start_time"],
                      validation_first_kickoff=validation[0][0]["start_time"],
                      test_first_kickoff=test[0][0]["start_time"],
                      split="chronological 60/20/20; equal kickoffs grouped; no test tuning",
                      calibration="not calibrated; probabilities require real-data validation")
        for name, subset in (("validation", validation), ("test", test)):
            y = [r[2] for r in subset]
            probs = model.predict_proba([r[1] for r in subset])
            report[name] = dict(model=score(y, probs, classes),
                                frequency_baseline=score(y, np.tile(prior, (len(y), 1)), classes))
        reports[sport] = report
        for match in current:
            if match["sport"] != sport or match["status"] != "scheduled" or match["start_time"] <= available_as_of:
                continue
            values, evidence = features(match, events, available_as_of)
            if min(evidence["historical_matches_a"], evidence["historical_matches_b"]) < 3:
                continue
            probabilities = dict(zip(model.classes_, map(float, model.predict_proba([values])[0])))
            if not math.isclose(sum(probabilities.values()), 1.0, abs_tol=1e-8):
                raise ValueError("Invalid prediction probability mass")
            predictions.append(dict(match_id=match["match_id"], sport=sport,
                team_a=match["team_a"], team_b=match["team_b"], start_time=match["start_time"],
                probabilities=probabilities, model="scaled_logistic_v1", data_kind=data_kind,
                as_of=available_as_of, evidence=evidence,
                limitations="Uncalibrated synthetic demonstration" if data_kind == "synthetic"
                else "Uncalibrated; validate on held-out real competitions"))
    return predictions, dict(data_kind=data_kind, sports=reports, available_as_of=available_as_of)
