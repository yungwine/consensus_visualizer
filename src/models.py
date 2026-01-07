from dataclasses import dataclass


@dataclass
class SlotData:
    valgroup_id: str
    slot: int
    is_empty: bool
    slot_start_est_ms: float
    block_id: str | None
    collator: int | str | None


@dataclass
class EventData:
    valgroup_id: str
    slot: int
    label: str
    kind: str
    t_ms: float
    validator: int | str | None = None
    t1_ms: float | None = None

    def get_color(self) -> str | None:
        from src.visualizer.style import COLOR_MAP

        return COLOR_MAP.get(self.label)

    def get_symbol(self) -> str:
        from src.visualizer.style import SYMBOL_MAP

        return SYMBOL_MAP.get(self.kind, "circle")


@dataclass
class ConsensusData:
    slots: list[SlotData]
    events: list[EventData]
