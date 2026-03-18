"""
StreamStation42 - Live Broadcast Scheduler.

Given a channel's lineup (ordered list of shows, bumpers, and commercials),
the scheduler answers: "what should be playing right now, and at what position
within that item?"

The broadcast runs as a continuous loop:
  - If reruns_enabled is True (default), the lineup repeats indefinitely.
  - If reruns_enabled is False, nothing plays after the last item ends.

Commercial breaks are inserted automatically between shows at the configured
break_interval_sec if any commercials exist for the channel.

Bumpers of type "transition" are inserted between shows automatically if
any bumpers exist for the channel.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

from . import channel as ch_mod


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ScheduledSegment:
    """A single segment in the resolved broadcast timeline."""
    segment_type: str           # 'show' | 'bumper' | 'commercial' | 'off-air'
    item_id: str                # DB row id of the show/bumper/commercial
    title: str
    duration: float             # seconds
    torrent_hash: str = ""
    file_path: str = ""
    metadata: dict = field(default_factory=dict)


@dataclass
class NowPlayingInfo:
    """What is currently playing and where in the segment we are."""
    segment: ScheduledSegment
    position_sec: float         # How many seconds into the current segment
    cycle_position_sec: float   # How many seconds into the full cycle
    cycle_duration_sec: float   # Total duration of one complete cycle
    segments: list[ScheduledSegment] = field(default_factory=list)
    next_segment: ScheduledSegment | None = None


# ---------------------------------------------------------------------------
# Timeline builder
# ---------------------------------------------------------------------------

def _build_timeline(channel_id: str, db_path: str = None) -> list[ScheduledSegment]:
    """
    Build the complete list of segments for one cycle of the channel broadcast.

    Inserts bumpers between shows and commercial breaks at the configured
    interval, then wraps show blocks from the lineup.
    """
    lineup = ch_mod.get_lineup(channel_id, db_path=db_path)
    bumpers = ch_mod.list_bumpers(channel_id, db_path=db_path)
    commercials = ch_mod.list_commercials(channel_id, db_path=db_path)

    # Pick the first transition bumper (if any) and first commercial (if any)
    transition_bumpers = [b for b in bumpers if b.get("bumper_type") == "transition"]
    intro_bumpers = [b for b in bumpers if b.get("bumper_type") == "intro"]
    outro_bumpers = [b for b in bumpers if b.get("bumper_type") == "outro"]

    # Determine commercial break interval (use the smallest among all commercials)
    break_interval = None
    if commercials:
        break_interval = min(c["break_interval_sec"] for c in commercials)

    segments: list[ScheduledSegment] = []

    # Optional intro bumper at the start of each cycle
    for b in intro_bumpers:
        segments.append(ScheduledSegment(
            segment_type="bumper",
            item_id=b["id"],
            title=b.get("title", "Channel Intro"),
            duration=b.get("duration", 5.0),
            torrent_hash=b.get("torrent_hash", ""),
            file_path=b.get("file_path", ""),
        ))

    time_since_last_break = 0.0

    for idx, item in enumerate(lineup):
        # Insert commercial break before this show if interval has elapsed
        if (break_interval is not None and
                time_since_last_break >= break_interval and
                commercials):
            for comm in commercials:
                segments.append(ScheduledSegment(
                    segment_type="commercial",
                    item_id=comm["id"],
                    title=comm.get("title", "Commercial"),
                    duration=comm.get("duration", 30.0),
                    torrent_hash=comm.get("torrent_hash", ""),
                    file_path=comm.get("file_path", ""),
                ))
            time_since_last_break = 0.0

        # The show itself
        if item["item_type"] == "show" and item.get("show_id"):
            seg = ScheduledSegment(
                segment_type="show",
                item_id=item["show_id"],
                title=item.get("show_title") or "Untitled Show",
                duration=item.get("show_duration") or 0.0,
                torrent_hash=item.get("show_torrent_hash") or "",
                file_path=item.get("show_file_path") or "",
                metadata=item.get("metadata", {}),
            )
            segments.append(seg)
            time_since_last_break += seg.duration

        # Insert transition bumper between shows (but not after the last one)
        if (transition_bumpers and idx < len(lineup) - 1
                and item["item_type"] == "show"):
            b = transition_bumpers[idx % len(transition_bumpers)]
            segments.append(ScheduledSegment(
                segment_type="bumper",
                item_id=b["id"],
                title=b.get("title", "Station ID"),
                duration=b.get("duration", 5.0),
                torrent_hash=b.get("torrent_hash", ""),
                file_path=b.get("file_path", ""),
            ))

    # Optional outro bumper at the end of each cycle
    for b in outro_bumpers:
        segments.append(ScheduledSegment(
            segment_type="bumper",
            item_id=b["id"],
            title=b.get("title", "Channel Outro"),
            duration=b.get("duration", 5.0),
            torrent_hash=b.get("torrent_hash", ""),
            file_path=b.get("file_path", ""),
        ))

    return segments


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_cycle_duration(segments: list[ScheduledSegment]) -> float:
    """Total duration (in seconds) of one complete broadcast cycle."""
    return sum(s.duration for s in segments)


def now_playing(channel_id: str, wall_clock_seconds: float,
                epoch_offset: float = 0.0,
                reruns_enabled: bool = True,
                db_path: str = None) -> NowPlayingInfo:
    """
    Return NowPlayingInfo for the given wall-clock time.

    Args:
        channel_id:          Target channel.
        wall_clock_seconds:  Unix timestamp (or any monotonic seconds value).
        epoch_offset:        Seconds to subtract from wall_clock before
                             computing position (allows syncing multiple peers).
        reruns_enabled:      Whether the lineup loops after the last item.
        db_path:             Optional path to the SQLite database.

    Returns:
        NowPlayingInfo – if the channel has no content, segment_type is
        'off-air'.
    """
    segments = _build_timeline(channel_id, db_path=db_path)

    off_air_segment = ScheduledSegment(
        segment_type="off-air",
        item_id="",
        title="Off Air",
        duration=0.0,
    )

    if not segments:
        return NowPlayingInfo(
            segment=off_air_segment,
            position_sec=0.0,
            cycle_position_sec=0.0,
            cycle_duration_sec=0.0,
            segments=[],
            next_segment=None,
        )

    cycle_duration = get_cycle_duration(segments)
    if cycle_duration <= 0:
        return NowPlayingInfo(
            segment=off_air_segment,
            position_sec=0.0,
            cycle_position_sec=0.0,
            cycle_duration_sec=cycle_duration,
            segments=segments,
            next_segment=None,
        )

    elapsed = wall_clock_seconds - epoch_offset

    if reruns_enabled:
        cycle_pos = math.fmod(elapsed, cycle_duration)
        if cycle_pos < 0:
            cycle_pos += cycle_duration
    else:
        cycle_pos = elapsed
        if cycle_pos >= cycle_duration:
            return NowPlayingInfo(
                segment=off_air_segment,
                position_sec=0.0,
                cycle_position_sec=cycle_pos,
                cycle_duration_sec=cycle_duration,
                segments=segments,
                next_segment=None,
            )

    # Walk segments to find which one covers cycle_pos
    acc = 0.0
    for i, seg in enumerate(segments):
        if acc + seg.duration > cycle_pos:
            position_in_seg = cycle_pos - acc
            next_seg = segments[(i + 1) % len(segments)] if reruns_enabled else (
                segments[i + 1] if i + 1 < len(segments) else None
            )
            return NowPlayingInfo(
                segment=seg,
                position_sec=position_in_seg,
                cycle_position_sec=cycle_pos,
                cycle_duration_sec=cycle_duration,
                segments=segments,
                next_segment=next_seg,
            )
        acc += seg.duration

    # Fallback (floating point edge case – treat as start of first segment)
    return NowPlayingInfo(
        segment=segments[0],
        position_sec=0.0,
        cycle_position_sec=0.0,
        cycle_duration_sec=cycle_duration,
        segments=segments,
        next_segment=segments[1] if len(segments) > 1 else None,
    )


def get_full_schedule(channel_id: str, db_path: str = None) -> list[dict]:
    """
    Return the resolved segment timeline as a list of dicts (for API responses).
    Each dict has an additional 'timeline_offset_sec' showing when in the cycle
    that segment starts.
    """
    segments = _build_timeline(channel_id, db_path=db_path)
    result = []
    acc = 0.0
    for seg in segments:
        result.append({
            "segment_type": seg.segment_type,
            "item_id": seg.item_id,
            "title": seg.title,
            "duration": seg.duration,
            "torrent_hash": seg.torrent_hash,
            "file_path": seg.file_path,
            "timeline_offset_sec": acc,
        })
        acc += seg.duration
    return result
