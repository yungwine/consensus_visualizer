from datetime import datetime, timezone
import plotly.graph_objects as go  # pyright: ignore[reportMissingTypeStubs]

from src.models import ConsensusData, EventData, SlotData


def to_datetime(t_ms: float) -> datetime:
    return datetime.fromtimestamp(t_ms / 1000, tz=timezone.utc)


def to_relative(t_ms: float, base_ms: float) -> float:
    return round(t_ms - base_ms, 6)


class DataFilter:
    def __init__(self, data: ConsensusData):
        self.data: ConsensusData = data

    def filter_slots(
        self, valgroup_id: str, slot_from: int, slot_to: int, show_empty: bool
    ) -> list[SlotData]:
        return [
            s
            for s in self.data.slots
            if s.valgroup_id == valgroup_id
            and slot_from <= s.slot <= slot_to
            and (show_empty or not s.is_empty)
        ]

    def filter_events(
        self,
        valgroup_id: str | None = None,
        slot: int | None = None,
        slots: set[int] | None = None,
        labels: set[str] | None = None,
        kinds: set[str] | None = None,
        has_validator: bool | None = None,
    ) -> list[EventData]:
        result: list[EventData] = []
        for e in self.data.events:
            if valgroup_id and e.valgroup_id != valgroup_id:
                continue
            if slot is not None and e.slot != slot:
                continue
            if slots and e.slot not in slots:
                continue
            if labels and e.label not in labels:
                continue
            if kinds and e.kind not in kinds:
                continue
            if has_validator is not None:
                if has_validator and e.validator is None:
                    continue
                if not has_validator and e.validator is not None:
                    continue
            result.append(e)
        return result

    def get_slot(self, valgroup_id: str, slot: int) -> SlotData | None:
        for s in self.data.slots:
            if s.valgroup_id == valgroup_id and s.slot == slot:
                return s
        return None

    @staticmethod
    def group_events_by_label(events: list[EventData]) -> dict[str, list[EventData]]:
        result: dict[str, list[EventData]] = {}
        for e in events:
            result.setdefault(e.label, []).append(e)
        return result


class SummaryFigureBuilder:
    def __init__(self, valgroup_id: str):
        self.valgroup_id: str = valgroup_id

    def build(
        self,
        segments: list[EventData],
        markers: list[EventData],
        slot_from: int,
        slot_to: int,
    ) -> go.Figure:
        fig = go.Figure()
        self._add_bars(fig, segments)
        self._add_markers(fig, markers)
        self._configure_layout(fig, slot_from, slot_to)
        return fig

    def _add_bars(self, fig: go.Figure, segments: list[EventData]) -> None:
        events_by_label = DataFilter.group_events_by_label(segments)

        for label in sorted(events_by_label.keys()):
            events = events_by_label[label]
            fig = fig.add_trace(  # pyright: ignore[reportUnknownMemberType]
                go.Bar(
                    orientation="h",
                    y=[str(e.slot) for e in events],
                    base=[to_datetime(e.t_ms) for e in events],
                    x=[e.t1_ms - e.t_ms if e.t1_ms else 0 for e in events],
                    name=label,
                    marker=dict(color=events[0].get_color()),
                    customdata=[
                        [self.valgroup_id, e.slot, e.t1_ms - e.t_ms if e.t1_ms else 0]
                        for e in events
                    ],
                    hovertemplate=f"valgroup={self.valgroup_id}<br>slot=%{{customdata[1]}}<br>segment={label}<br>start=%{{base|%H:%M:%S.%f}}<br>dt=%{{customdata[2]:.3f}}ms<extra></extra>",
                )
            )

    def _add_markers(self, fig: go.Figure, markers: list[EventData]) -> None:
        markers_by_label: dict[str, list[EventData]] = {}
        for m in markers:
            markers_by_label.setdefault(m.label, []).append(m)

        for label in sorted(markers_by_label.keys()):
            events = markers_by_label[label]
            fig = fig.add_trace(  # pyright: ignore[reportUnknownMemberType]
                go.Scatter(
                    x=[to_datetime(e.t_ms) for e in events],
                    y=[str(e.slot) for e in events],
                    mode="markers",
                    marker=dict(
                        # size=11,
                        symbol=[e.get_symbol() for e in events],
                        color=events[0].get_color(),
                    ),
                    name=label,
                    legendgroup=f"m:{label}",
                    customdata=[[self.valgroup_id, e.slot, to_datetime(e.t_ms).strftime("%H:%M:%S.%f")] for e in events],
                    hovertemplate=f"valgroup={self.valgroup_id}<br>slot=%{{customdata[1]}}<br>marker={label}<br>t=%{{customdata[2]}}<extra></extra>",
                )
            )

    @staticmethod
    def _configure_layout(fig: go.Figure, slot_from: int, slot_to: int) -> None:
        _ = fig.update_layout(  # pyright: ignore[reportUnknownMemberType]
            height=600,
            barmode="overlay",
            bargap=0.25,
            xaxis=dict(title="Time (UTC)", type="date"),
            yaxis=dict(
                title="Slot",
                type="category",
                categoryorder="array",
                categoryarray=[str(s) for s in range(slot_to, slot_from - 1, -1)],
                tickmode="auto",
                nticks=25,
            ),
            dragmode="pan",
        )


class DetailFigureBuilder:
    def __init__(self, valgroup_id: str, slot: int):
        self.valgroup_id: str = valgroup_id
        self.slot: int = slot

    def build(
        self,
        slot_data: SlotData,
        events: list[EventData],
        markers: list[EventData],
        time_mode: str,
    ) -> go.Figure:
        slot_start_ms = self._get_slot_start(markers, events)
        x_title = "t - slot_start_est (ms)" if time_mode == "rel" else "Time (UTC)"

        fig = go.Figure()
        self._add_baseline_markers(fig, markers, time_mode, slot_start_ms)
        self._add_validator_events(fig, events, time_mode, slot_start_ms)
        self._configure_layout(fig, slot_data, events, x_title, time_mode)
        return fig

    @staticmethod
    def _get_slot_start(markers: list[EventData], events: list[EventData]) -> float:
        for m in markers:
            if m.label == "slot_start_est":
                return m.t_ms
        return min(e.t_ms for e in events) if events else 0

    @staticmethod
    def _add_baseline_markers(
        fig: go.Figure, markers: list[EventData], time_mode: str, slot_start: float
    ) -> None:
        for m in markers:
            x = (
                to_datetime(m.t_ms)
                if time_mode == "abs"
                else to_relative(m.t_ms, slot_start)
            )

            fig = fig.add_vline(x=x, line_width=1, line_dash="dot")  # pyright: ignore[reportUnknownMemberType]
            fig = fig.add_trace(  # pyright: ignore[reportUnknownMemberType]
                go.Scatter(
                    x=[x],
                    y=["__slot__"],
                    mode="markers",
                    marker=dict(
                        size=10,
                        symbol=m.get_symbol(),
                        color=m.get_color(),
                    ),
                    name=m.label,
                    legendgroup=f"slot:{m.label}",
                    showlegend=True,
                    customdata=[[m.label, x]],
                    hovertemplate="slot: %{customdata[0]}<br>"
                                  + (
                                    "t=%{customdata[1]|%H:%M:%S.%f}<br>"
                                    if time_mode == "abs"
                                    else "t=%{customdata[1]}ms<br>"
                                    )
                                    +"<extra></extra>",
                )
            )

    @staticmethod
    def _infer_continuous_events(events: list[EventData]) -> list[EventData]:
        # group by (validator, label)
        event_map: dict[tuple[int | str | None, str], EventData] = {}
        for e in events:
            event_map[(e.validator, e.label)] = e

        inferred_events: list[EventData] = []
        for e in events:
            if e.label == "collate_started":
                end_event = event_map.get((e.validator, "collate_finished"))
                if end_event:
                    inferred_events.append(
                        EventData(
                            valgroup_id=e.valgroup_id,
                            slot=e.slot,
                            validator=e.validator,
                            label="collation",
                            kind=e.kind,
                            t_ms=e.t_ms,
                            t1_ms=end_event.t_ms,
                        )
                    )
            elif e.label == "validate_started":
                end_event = event_map.get((e.validator, "validate_finished"))
                if end_event:
                    inferred_events.append(
                        EventData(
                            valgroup_id=e.valgroup_id,
                            slot=e.slot,
                            validator=e.validator,
                            label="block_validation",
                            kind=e.kind,
                            t_ms=e.t_ms,
                            t1_ms=end_event.t_ms,
                        )
                    )
            elif e.label == "notarize_observed":
                end_event = event_map.get((e.validator, "finalize_observed"))
                if end_event:
                    inferred_events.append(
                        EventData(
                            valgroup_id=e.valgroup_id,
                            slot=e.slot,
                            validator=e.validator,
                            label="finalization",
                            kind=e.kind,
                            t_ms=e.t_ms,
                            t1_ms=end_event.t_ms,
                        )
                    )
        return inferred_events

    def _add_validator_events(
        self, fig: go.Figure, events: list[EventData], time_mode: str, slot_start: float
    ) -> None:
        inferred_events = self._infer_continuous_events(events)
        events_by_label = DataFilter.group_events_by_label(events + inferred_events)

        for label in sorted(events_by_label.keys()):
            if label not in (
                "block_validation",
                "finalization",
                "collation",
                "skip_observed",
                "candidate_received",
            ):
                continue
            label_events = events_by_label[label]

            if time_mode == "abs":
                base = [to_datetime(e.t_ms) for e in label_events]
                x = [e.t1_ms - e.t_ms if e.t1_ms else 0 for e in label_events]
            else:
                base = [to_relative(e.t_ms, slot_start) for e in label_events]
                x = [e.t1_ms - e.t_ms if e.t1_ms else 0 for e in label_events]

            kwargs = dict(
                name=label,
                legendgroup=f"ev:{label}",
                customdata=[
                    [
                        self.valgroup_id,
                        self.slot,
                        e.validator,
                        label,
                        e.kind,
                        e.t1_ms - e.t_ms if e.t1_ms else 0,
                        b
                    ]
                    for e, b in zip(label_events, base)
                ],
            )

            if label not in ("skip_observed", "candidate_received"):
                fig = fig.add_trace(  # pyright: ignore[reportUnknownMemberType]
                    go.Bar(
                        orientation="h",
                        base=base,
                        x=x,
                        y=[e.validator for e in label_events],
                        marker=dict(color=label_events[0].get_color()),
                        hovertemplate=(
                            f"valgroup={self.valgroup_id}<br>slot={self.slot}<br>"
                            + f"validator=%{{customdata[2]}}<br>event={label} (kind=%{{customdata[4]}})<br>"
                            + (
                                "start=%{base|%H:%M:%S.%f}<br>"
                                if time_mode == "abs"
                                else "start=%{base}ms<br>"
                            )
                            + "dt=%{customdata[5]:.3f}ms<extra></extra>"
                        ),
                        **kwargs,
                    )
                )
            else:
                fig = fig.add_trace(  # pyright: ignore[reportUnknownMemberType]
                    go.Scatter(
                        x=base,
                        y=[e.validator for e in label_events],
                        mode="markers",
                        marker=dict(
                            size=10,
                            symbol=label_events[0].get_symbol(),
                            color=label_events[0].get_color(),
                        ),
                        hovertemplate=(
                            f"valgroup={self.valgroup_id}<br>slot={self.slot}<br>"
                            + f"validator=%{{customdata[2]}}<br>event={label} (kind=%{{customdata[4]}})<br>"
                            + (
                                "t=%{customdata[6]|%H:%M:%S.%f}<br><extra></extra>"
                                if time_mode == "abs"
                                else "t=%{x}ms<br><extra></extra>"
                            )
                        ),
                        **kwargs,
                    )
                )

    def _configure_layout(
        self,
        fig: go.Figure,
        slot_data: SlotData,
        events: list[EventData],
        x_title: str,
        time_mode: str,
    ) -> None:
        title_parts = [f"Detail — ({self.valgroup_id}) slot {self.slot}"]
        if slot_data.is_empty:
            title_parts.append("empty")
        if slot_data.block_id:
            title_parts.append(f"block={slot_data.block_id}")
        if slot_data.collator is not None:
            title_parts.append(f"collator={slot_data.collator}")

        validators = sorted({e.validator for e in events if e.validator is not None})

        _ = fig.update_layout(  # pyright: ignore[reportUnknownMemberType]
            title=" · ".join(title_parts),
            height=820,
            hovermode="closest",
            barmode="overlay",
            xaxis=dict(
                title=x_title,
                type="date" if time_mode == "abs" else "linear",
                rangeslider=dict(visible=True) if time_mode == "abs" else None,
            ),
            yaxis=dict(
                title="Validator",
                type="category",
                categoryorder="array",
                categoryarray=["__slot__"] + validators,
            ),
            margin=dict(l=130, r=20, t=60, b=55),
            dragmode="pan",
        )


class FigureBuilder:
    def __init__(self, data: ConsensusData):
        self.data: ConsensusData = data
        self.filter: DataFilter = DataFilter(data)

    def build_summary(
        self,
        valgroup_id: str,
        slot_from: int,
        slot_to: int,
        show_empty: bool,
    ) -> go.Figure:
        slots = self.filter.filter_slots(valgroup_id, slot_from, slot_to, show_empty)
        slot_set = {s.slot for s in slots}

        segments = self.filter.filter_events(
            valgroup_id=valgroup_id,
            slots=slot_set,
            kinds={"phase"},
        )

        markers = self.filter.filter_events(
            valgroup_id=valgroup_id,
            slots=slot_set,
            labels={"slot_start_est", "finalize_observed_by_next_leader"},
        )

        builder = SummaryFigureBuilder(valgroup_id)
        return builder.build(segments, markers, slot_from, slot_to)

    def build_detail(
        self,
        valgroup_id: str,
        slot: int,
        event_labels: list[str] | None,
        time_mode: str,
    ) -> tuple[go.Figure, list[dict[str, str]]]:
        slot_data = self.filter.get_slot(valgroup_id, slot)
        if not slot_data:
            return go.Figure().update_layout(title="No slot selected"), []  # pyright: ignore[reportUnknownMemberType]

        events = self.filter.filter_events(
            valgroup_id=valgroup_id,
            slot=slot,
            has_validator=True,
        )

        labels = sorted(set(e.label for e in events))
        options = [{"label": l, "value": l} for l in labels]

        if not events:
            return go.Figure().update_layout(  # pyright: ignore[reportUnknownMemberType]
                title=f"{valgroup_id} slot {slot}: no events"
            ), options

        if event_labels:
            events = [e for e in events if e.label in event_labels]

        marker_labels = {
            "slot_start_est",
            "finalize_observed_by_next_leader",
            "skip_reached",
            "notarize_reached",
            "finalize_reached",
        }
        markers = self.filter.filter_events(
            valgroup_id=valgroup_id,
            slot=slot,
            labels=marker_labels,
        )

        builder = DetailFigureBuilder(valgroup_id, slot)
        return builder.build(slot_data, events, markers, time_mode), options
