from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable, List, Optional

import yaml


def _compile_patterns(items: Iterable[str]) -> List[re.Pattern[str]]:
    return [re.compile(pat, re.IGNORECASE) for pat in items]


def _match_any(patterns: List[re.Pattern[str]], text: str) -> bool:
    return any(pat.search(text) for pat in patterns)


@dataclass(frozen=True)
class PlayTypePatterns:
    turnover: List[re.Pattern[str]]
    def_rebound: List[re.Pattern[str]]
    off_rebound: List[re.Pattern[str]]
    made_fg: List[re.Pattern[str]]
    missed_fg: List[re.Pattern[str]]
    made_ft: List[re.Pattern[str]]
    missed_ft: List[re.Pattern[str]]
    ft_last: List[re.Pattern[str]]
    period_end: List[re.Pattern[str]]
    shot: List[re.Pattern[str]]
    free_throw: List[re.Pattern[str]]

    @classmethod
    def from_yaml(cls, path: str) -> "PlayTypePatterns":
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        return cls(
            turnover=_compile_patterns(raw.get("turnover", [])),
            def_rebound=_compile_patterns(raw.get("def_rebound", [])),
            off_rebound=_compile_patterns(raw.get("off_rebound", [])),
            made_fg=_compile_patterns(raw.get("made_fg", [])),
            missed_fg=_compile_patterns(raw.get("missed_fg", [])),
            made_ft=_compile_patterns(raw.get("made_ft", [])),
            missed_ft=_compile_patterns(raw.get("missed_ft", [])),
            ft_last=_compile_patterns(raw.get("ft_last", [])),
            period_end=_compile_patterns(raw.get("period_end", [])),
            shot=_compile_patterns(raw.get("shot", [])),
            free_throw=_compile_patterns(raw.get("free_throw", [])),
        )


@dataclass
class PlayFlags:
    is_turnover: bool = False
    is_def_rebound: bool = False
    is_off_rebound: bool = False
    is_made_fg: bool = False
    is_missed_fg: bool = False
    is_made_ft: bool = False
    is_missed_ft: bool = False
    is_last_ft: bool = False
    is_period_end: bool = False

    @property
    def is_fga(self) -> bool:
        return self.is_made_fg or self.is_missed_fg

    @property
    def is_fta(self) -> bool:
        return self.is_made_ft or self.is_missed_ft

    @property
    def ends_possession(self) -> bool:
        return self.is_turnover or self.is_def_rebound or self.is_made_fg or (self.is_made_ft and self.is_last_ft) or self.is_period_end


class PlayClassifier:
    def __init__(self, patterns: PlayTypePatterns) -> None:
        self.patterns = patterns

    def classify(
        self,
        play_type: Optional[str],
        play_text: Optional[str],
        scoring_play: Optional[object],
        shooting_play: Optional[object] = None,
        score_value: Optional[object] = None,
    ) -> PlayFlags:
        text = " ".join([play_type or "", play_text or ""]).strip()
        if not text:
            return PlayFlags()
        is_scoring = bool(scoring_play)
        is_shot = bool(shooting_play) if shooting_play is not None else _match_any(self.patterns.shot, text)
        is_ft = False
        try:
            is_ft = float(score_value) == 1.0
        except (TypeError, ValueError):
            is_ft = _match_any(self.patterns.free_throw, text)
        if is_ft:
            is_shot = False
        return PlayFlags(
            is_turnover=_match_any(self.patterns.turnover, text),
            is_def_rebound=_match_any(self.patterns.def_rebound, text),
            is_off_rebound=_match_any(self.patterns.off_rebound, text),
            is_made_fg=is_shot and is_scoring,
            is_missed_fg=is_shot and not is_scoring,
            is_made_ft=is_ft and is_scoring,
            is_missed_ft=is_ft and not is_scoring,
            is_last_ft=_match_any(self.patterns.ft_last, text),
            is_period_end=_match_any(self.patterns.period_end, text),
        )
