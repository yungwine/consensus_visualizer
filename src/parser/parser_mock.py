import random
from datetime import datetime, timedelta, timezone
from typing import final, override

from src.parser.parser_base import Parser
from src.models import ConsensusData, SlotData, EventData


@final
class ParserMock(Parser):
    def __init__(
        self,
        n_groups: int = 2,
        n_slots: int = 120,
        n_validators: int = 12,
        start_utc: datetime | None = None,
        collate_gap_ms: int = 100,
        phase_ms: int = 50,
        finalize_lag_ms: int = 200,
        empty_every: int = 11,
    ):
        self.n_groups = n_groups
        self.n_slots = n_slots
        self.n_validators = n_validators
        self.start_utc = start_utc or datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        self.collate_gap_ms = collate_gap_ms
        self.phase_ms = phase_ms
        self.finalize_lag_ms = finalize_lag_ms
        self.empty_every = empty_every

    @override
    def parse(self) -> ConsensusData:
        slots: list[SlotData] = []
        events: list[EventData] = []

        for group_idx in range(self.n_groups):
            group_id = "mc" if group_idx == 0 else f"shard:{group_idx}"
            validators = [f"{group_id}:v{i:02d}" for i in range(self.n_validators)]
            group_start = self.start_utc + timedelta(seconds=5 * group_idx)

            for slot_idx in range(self.n_slots):
                is_empty = slot_idx % self.empty_every == 0

                jitter = ((slot_idx * 37 + group_idx * 13) % 51) - 25
                slot_start = group_start + timedelta(
                    milliseconds=slot_idx * self.collate_gap_ms + jitter
                )

                collate_start = group_start + timedelta(
                    milliseconds=slot_idx * self.collate_gap_ms
                )
                collate_end = collate_start + timedelta(milliseconds=self.phase_ms)
                notarize_time = collate_end + timedelta(milliseconds=self.phase_ms)
                finalize_time = notarize_time + timedelta(milliseconds=self.phase_ms)
                finalize_observed = collate_start + timedelta(
                    milliseconds=self.finalize_lag_ms
                )

                slots.append(
                    SlotData(
                        valgroup_id=group_id,
                        slot=slot_idx,
                        is_empty=is_empty,
                        slot_start_est_ms=self._to_ms(slot_start),
                        block_id_ext=None if is_empty else f"{group_id}-B{slot_idx:06d}",
                        collator=None
                        if is_empty
                        else validators[slot_idx % self.n_validators],
                    )
                )

                events.append(
                    EventData(
                        valgroup_id=group_id,
                        slot=slot_idx,
                        label="slot_start_est",
                        kind="estimate",
                        t_ms=self._to_ms(slot_start),
                        validator=None,
                        t1_ms=None,
                    )
                )

                if is_empty:
                    self._add_empty_slot(
                        group_id, slot_idx, validators, collate_start, events
                    )
                else:
                    self._add_non_empty_slot(
                        group_id,
                        slot_idx,
                        validators,
                        collate_start,
                        collate_end,
                        notarize_time,
                        finalize_time,
                        finalize_observed,
                        events,
                    )
        return ConsensusData(slots=slots, events=events)

    def _add_empty_slot(
        self,
        group_id: str,
        slot: int,
        validators: list[str],
        collate_start: datetime,
        events: list[EventData],
    ) -> None:
        skip_time = collate_start + timedelta(milliseconds=self.phase_ms * 2)
        finalize_obs = skip_time + timedelta(milliseconds=self.phase_ms)

        events.extend(
            [
                EventData(
                    valgroup_id=group_id,
                    slot=slot,
                    label="skip_reached",
                    kind="reached",
                    t_ms=self._to_ms(skip_time),
                    validator=None,
                    t1_ms=None,
                ),
                EventData(
                    valgroup_id=group_id,
                    slot=slot,
                    label="finalize_observed_by_next_leader",
                    kind="observed",
                    t_ms=self._to_ms(finalize_obs),
                    validator=None,
                    t1_ms=None,
                ),
            ]
        )

        for i, validator in enumerate(validators):
            lag = 5 + (i * 7 + slot * 3) % 80
            events.append(
                EventData(
                    valgroup_id=group_id,
                    slot=slot,
                    validator=validator,
                    label="skip_observed",
                    kind="observed",
                    t_ms=self._to_ms(skip_time) + lag,
                    t1_ms=self._to_ms(skip_time) + lag,
                )
            )

    def _add_non_empty_slot(
        self,
        group_id: str,
        slot: int,
        validators: list[str],
        collate_start: datetime,
        collate_end: datetime,
        notarize_time: datetime,
        finalize_time: datetime,
        finalize_observed: datetime,
        events: list[EventData],
    ) -> None:
        collator = validators[slot % self.n_validators]

        events.extend(
            [
                EventData(
                    valgroup_id=group_id,
                    slot=slot,
                    label="collate",
                    kind="phase",
                    t_ms=self._to_ms(collate_start),
                    validator=None,
                    t1_ms=self._to_ms(collate_end),
                ),
                EventData(
                    valgroup_id=group_id,
                    slot=slot,
                    label="notarize",
                    kind="phase",
                    t_ms=self._to_ms(collate_end),
                    validator=None,
                    t1_ms=self._to_ms(notarize_time),
                ),
                EventData(
                    valgroup_id=group_id,
                    slot=slot,
                    label="finalize",
                    kind="phase",
                    t_ms=self._to_ms(notarize_time),
                    validator=None,
                    t1_ms=self._to_ms(finalize_time),
                ),
            ]
        )

        events.extend(
            [
                EventData(
                    valgroup_id=group_id,
                    slot=slot,
                    label="notarize_reached",
                    kind="reached",
                    t_ms=self._to_ms(notarize_time),
                    validator=None,
                    t1_ms=None,
                ),
                EventData(
                    valgroup_id=group_id,
                    slot=slot,
                    label="finalize_reached",
                    kind="reached",
                    t_ms=self._to_ms(finalize_time),
                    validator=None,
                    t1_ms=None,
                ),
                EventData(
                    valgroup_id=group_id,
                    slot=slot,
                    label="finalize_observed_by_next_leader",
                    kind="observed",
                    t_ms=self._to_ms(finalize_observed),
                    validator=None,
                    t1_ms=None,
                ),
            ]
        )

        for i, validator in enumerate(validators):
            receive_time = (
                self._to_ms(collate_end) + 10 + (i % 5) * 7 + (slot * 11 + i * 3) % 20
            )
            validate_time = receive_time + 15 + (i * 5 + slot) % 25
            notarize_obs = (
                self._to_ms(notarize_time) + 20 + (i % 4) * 9 + (slot * 7 + i * 13) % 60
            )
            finalize_obs = (
                self._to_ms(finalize_time) + 30 + (i % 6) * 8 + (slot * 5 + i * 17) % 80
            )

            if validator == collator:
                events.extend(
                    [
                        EventData(
                            valgroup_id=group_id,
                            slot=slot,
                            validator=validator,
                            label="collate_started",
                            kind="local",
                            t_ms=self._to_ms(collate_start),
                            t1_ms=None,
                        ),
                        EventData(
                            valgroup_id=group_id,
                            slot=slot,
                            validator=validator,
                            label="collate_finished",
                            kind="local",
                            t_ms=self._to_ms(collate_end),
                            t1_ms=None,
                        ),
                    ]
                )

            events.extend(
                [
                    EventData(
                        valgroup_id=group_id,
                        slot=slot,
                        validator=validator,
                        label="candidate_received",
                        kind="local",
                        t_ms=receive_time - random.randint(1, 50),
                    ),
                    EventData(
                        valgroup_id=group_id,
                        slot=slot,
                        validator=validator,
                        label="validate_started",
                        kind="local",
                        t_ms=receive_time,
                        t1_ms=None,
                    ),
                    EventData(
                        valgroup_id=group_id,
                        slot=slot,
                        validator=validator,
                        label="validate_finished",
                        kind="local",
                        t_ms=validate_time,
                        t1_ms=None,
                    ),
                    EventData(
                        valgroup_id=group_id,
                        slot=slot,
                        validator=validator,
                        label="notarize_observed",
                        kind="local",
                        t_ms=notarize_obs,
                        t1_ms=None,
                    ),
                    EventData(
                        valgroup_id=group_id,
                        slot=slot,
                        validator=validator,
                        label="finalize_observed",
                        kind="local",
                        t_ms=finalize_obs,
                        t1_ms=None,
                    ),
                ]
            )

    @staticmethod
    def _to_ms(dt: datetime) -> int:
        return int(dt.timestamp() * 1000)
