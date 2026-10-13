"""Bounded private provenance for a rejected canonical provider row.

This record identifies an archived JSON document, never a validation message,
request header, team name or public match fact. A rejection does not publish data.
"""

from dataclasses import asdict, dataclass
import re


@dataclass(frozen=True)
class RowRejection:
    provider: str
    endpoint: str
    archive_path: str
    archive_sha256: str
    row_index: int
    category: str = "invalid_match_event"

    def __post_init__(self):
        endpoint = {
            "football-data": r"competitions/[A-Z0-9]{1,12}/matches",
            "pandascore": r"valorant/matches/(past|upcoming|running)\?page=([1-9]|10)&per_page=100",
        }.get(self.provider)
        if (endpoint is None or not isinstance(self.endpoint, str)
                or not re.fullmatch(endpoint, self.endpoint)
                or not isinstance(self.archive_path, str)
                or not re.fullmatch(r"provider_raw/" + re.escape(self.provider)
                                    + r"/\d{4}-\d{2}-\d{2}/[0-9a-f]{32}\.json", self.archive_path)
                or not isinstance(self.archive_sha256, str)
                or not re.fullmatch(r"[0-9a-f]{64}", self.archive_sha256)
                or type(self.row_index) is not int or not 0 <= self.row_index < 1_000_000
                or self.category != "invalid_match_event"):
            raise ValueError("Invalid provider row rejection provenance")

    def record(self):
        return asdict(self)


class RejectedProviderRow(ValueError):
    def __init__(self, rejection: RowRejection):
        if not isinstance(rejection, RowRejection):
            raise ValueError("Invalid provider row rejection provenance")
        self.rejection = rejection
        super().__init__("invalid_provider_payload")
