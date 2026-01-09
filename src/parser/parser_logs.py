import dataclasses
import re
from datetime import datetime
from pathlib import Path
from typing import final, override

from src.parser.parser_base import Parser
from src.models import ConsensusData, SlotData, EventData

type slot_id_type = tuple[str, int]


@dataclasses.dataclass
class VoteData:
    vote: str
    t_ms: float
    v_id: int
    weight: int


TARGET_TO_LABEL = {
    "CandidateReceived": "candidate_received",
    "CollateStarted": "collate_started",
    "CollateFinished": "collate_finished",
    "ValidateStarted": "validate_started",
    "ValidateFinished": "validate_finished",
    "NotarObserved": "notarize_observed",
    "FinalObserved": "finalize_observed",
}


@final
class ParserLogs(Parser):
    def __init__(self, logs_path: list[Path]):
        self.logs_path = logs_path
        self.slots: dict[slot_id_type, SlotData] = {}
        self.collated: dict[slot_id_type, dict[str, EventData]] = {}
        self.votes: dict[slot_id_type, dict[str, list[VoteData]]] = {}
        self.total_weights: dict[str, int] = {}
        self.slot_leaders: dict[slot_id_type, int] = {}
        self.events: list[EventData] = []

    def _parse_stats_target_reached(
        self,
        line: str,
        t_ms: float,
        v_group: str,
        v_id: int,
    ):
        stats_match = re.search(
            r"target=(\w+),\s*slot=(\d+),\s*timestamp=([\d.]+)", line
        )
        if not stats_match:
            return

        target = stats_match.group(1)
        slot = int(stats_match.group(2))
        slot_id = (v_group, slot)

        if slot_id not in self.slots:
            self.slots[slot_id] = SlotData(
                valgroup_id=v_group,
                slot=slot,
                is_empty=False,
                slot_start_est_ms=t_ms,
                block_id=None,
                collator=None,
            )

        self.slots[slot_id].slot_start_est_ms = min(
            t_ms, self.slots[slot_id].slot_start_est_ms
        )

        if target == "CollateStarted":
            self.slots[slot_id].collator = v_id

        if (
            target == "FinalObserved"
            and self.slot_leaders.get((v_group, slot + 1)) == v_id
        ):
            self.events.append(
                EventData(
                    valgroup_id=v_group,
                    slot=slot,
                    label="finalize_observed_by_next_leader",
                    kind="observed",
                    t_ms=t_ms,
                    t1_ms=None,
                )
            )

        if target in TARGET_TO_LABEL:
            label = TARGET_TO_LABEL[target]
            ev = EventData(
                valgroup_id=v_group,
                slot=slot,
                label=label,
                kind="local",
                t_ms=t_ms,
                validator=v_id,
                t1_ms=None,
            )
            self.events.append(ev)

            if label in ("collate_started", "collate_finished"):
                self.collated.setdefault(slot_id, {})[label] = ev

    def _parse_skip_vote(self, line: str, t_ms: float, v_group: str, v_id: int):
        slot_match = re.search(r"slot=(\d+)", line)
        assert slot_match is not None

        slot = int(slot_match.group(1))
        slot_id = (v_group, slot)

        self.events.append(
            EventData(
                valgroup_id=v_group,
                slot=slot,
                label="skip_observed",
                kind="local",
                validator=v_id,
                t_ms=t_ms,
            )
        )

        if slot_id not in self.slots:
            self.slots[slot_id] = SlotData(
                valgroup_id=v_group,
                slot=slot,
                is_empty=True,
                slot_start_est_ms=t_ms,
                block_id=None,
                collator=None,
            )
        else:
            self.slots[slot_id].is_empty = True

    def _parse_publish_event(
        self, line: str, t_ms: float, v_group: str, v_id: int, v_weights: dict[str, int]
    ):
        if "BroadcastVote" in line and "SkipVote" not in line:
            slot_match = re.search(r"id=\{(\d+)", line)
            assert slot_match is not None
            slot = int(slot_match.group(1))
            slot_id = (v_group, slot)

            vote_match = re.search(r"vote=(\w+)", line)
            assert vote_match is not None
            vote = vote_match.group(1)

            self.votes.setdefault(slot_id, {}).setdefault(vote, []).append(
                VoteData(vote=vote, t_ms=t_ms, v_id=v_id, weight=v_weights[v_group])
            )

        elif "OurLeaderWindowStarted" in line:
            start_slot_match = re.search(r"start_slot=(\d+)", line)
            assert start_slot_match is not None
            start_slot = int(start_slot_match.group(1))

            end_slot_match = re.search(r"end_slot=(\d+)", line)
            assert end_slot_match is not None
            end_slot = int(end_slot_match.group(1))

            for s in range(start_slot, end_slot):
                self.slot_leaders[(v_group, s)] = v_id

    def _infer_slot_events(self):
        for slot_id, slot_data in self.slots.items():
            self.events.append(
                EventData(
                    valgroup_id=slot_data.valgroup_id,
                    slot=slot_data.slot,
                    label="slot_start_est",
                    kind="estimate",
                    t_ms=slot_data.slot_start_est_ms,
                )
            )

            collate_start = None
            collate_end = None
            if (
                slot_id in self.collated
                and "collate_started" in self.collated[slot_id]
                and "collate_finished" in self.collated[slot_id]
            ):
                collate_start = self.collated[slot_id]["collate_started"].t_ms
                collate_end = self.collated[slot_id]["collate_finished"].t_ms
                self.events.append(
                    EventData(
                        valgroup_id=slot_data.valgroup_id,
                        slot=slot_data.slot,
                        label="collate",
                        kind="phase",
                        t_ms=collate_start,
                        t1_ms=collate_end,
                    )
                )

            total_weight = self.total_weights[slot_id[0]]
            weight_threshold = (total_weight * 2) // 3 + 1

            notarize_reached = None
            if slot_id in self.votes and "NotarizeVote" in self.votes[slot_id] and collate_end is not None:
                notarize_reached = self._process_vote_threshold(
                    slot_data=slot_data,
                    votes=self.votes[slot_id]["NotarizeVote"],
                    weight_threshold=weight_threshold,
                    label="notarize",
                    phase_start=collate_end,
                )

            if (
                slot_id in self.votes
                and "FinalizeVote" in self.votes[slot_id]
                and notarize_reached is not None
            ):
                _ = self._process_vote_threshold(
                    slot_data=slot_data,
                    votes=self.votes[slot_id]["FinalizeVote"],
                    weight_threshold=weight_threshold,
                    label="finalize",
                    phase_start=notarize_reached,
                )

    def _process_vote_threshold(
        self,
        slot_data: SlotData,
        votes: list[VoteData],
        weight_threshold: int,
        label: str,
        phase_start: float,
    ) -> float | None:
        current_weight = 0
        sorted_votes = sorted(votes, key=lambda x: x.t_ms)

        for vote in sorted_votes:
            current_weight += vote.weight
            if current_weight >= weight_threshold:
                self.events.append(
                    EventData(
                        valgroup_id=slot_data.valgroup_id,
                        slot=slot_data.slot,
                        label=f"{label}_reached",
                        kind="reached",
                        t_ms=vote.t_ms,
                        validator=None,
                        t1_ms=None,
                    )
                )
                self.events.append(
                    EventData(
                        valgroup_id=slot_data.valgroup_id,
                        slot=slot_data.slot,
                        label=label,
                        kind="phase",
                        t_ms=phase_start,
                        t1_ms=vote.t_ms,
                    )
                )
                return vote.t_ms

        return None

    @staticmethod
    def _extract_timestamp(line: str) -> float | None:
        timestamp_match = re.search(
            r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6})\]", line
        )
        if not timestamp_match:
            return None
        timestamp_str = timestamp_match.group(1)
        dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
        return dt.timestamp() * 1000

    @staticmethod
    def _extract_valgroup(line: str) -> str | None:
        valgroup_match = re.search(r"valgroup\(([^)]+)\)\.(\d+)", line)
        if not valgroup_match:
            return None
        v_group = f"{valgroup_match.group(1)}.{valgroup_match.group(2)}"
        return v_group

    def _parse_validator_info(
        self,
        line: str,
        v_group: str,
        v_groups: dict[str, int],
        v_weights: dict[str, int],
    ):
        if "We are validator" not in line:
            return

        validator_match = re.search(r"We are validator (\d+)", line)
        assert validator_match is not None
        v_groups[v_group] = int(validator_match.group(1))

        weight_match = re.search(r"with weight (\d+)", line)
        assert weight_match is not None
        v_weights[v_group] = int(weight_match.group(1))

        total_weight_match = re.search(r"out of (\d+)", line)
        assert total_weight_match is not None
        self.total_weights[v_group] = int(total_weight_match.group(1))

    def _process_log_line(
        self,
        line: str,
        v_group: str,
        t_ms: float,
        v_groups: dict[str, int],
        v_weights: dict[str, int],
    ):
        v_id = v_groups.get(v_group)
        if v_id is None:
            return

        if "StatsTargetReached" in line:
            self._parse_stats_target_reached(line, t_ms, v_group, v_id)
        elif "Obtained certificate for SkipVote" in line:
            self._parse_skip_vote(line, t_ms, v_group, v_id)
        elif "Published event" in line:
            self._parse_publish_event(line, t_ms, v_group, v_id, v_weights)

    @override
    def parse(self) -> ConsensusData:
        for log_file in self.logs_path:
            data = log_file.read_text().splitlines()

            v_groups: dict[str, int] = {}
            v_weights: dict[str, int] = {}

            for line in data:
                t_ms = self._extract_timestamp(line)
                if t_ms is None:
                    continue

                v_group = self._extract_valgroup(line)
                if v_group is None:
                    continue

                self._parse_validator_info(line, v_group, v_groups, v_weights)

                self._process_log_line(line, v_group, t_ms, v_groups, v_weights)

        self._infer_slot_events()

        return ConsensusData(slots=list(self.slots.values()), events=self.events)
