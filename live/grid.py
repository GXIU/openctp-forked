"""CN 3-minute session-aware bar grid, matching cn_rst3 (obgx) conventions.

Replicated from obgx `cn_rst3/src/time_replay.rs` (`cn_bar_endtimes`) and
`streaming_bars.rs` (`BarGrid::window_index`):

- Bar windows are labeled by their END time, in nanoseconds past midnight.
- Morning: 09:30 pinned, then +3m through 11:30 → 41 endtimes.
- Afternoon: 13:00 is skipped ("no ticks land there after lunch"); 13:03,
  +3m through 14:57 → 39 endtimes, then 15:00 pinned. Total = 81.
- The 09:30 bar covers (09:15, 09:30] (call-auction fold-in); the 15:00 bar
  covers (14:57, 15:00] plus close-auction prints at exactly 15:00 and late
  stragglers (unconditional clamp — streaming_bars.rs window_index).
- Window assignment is `partition_point(|end| end <= ts)` clamped to the last
  window: windows are [start, end), a tick at exactly an endtime belongs to
  the NEXT window, lunch-gap timestamps attach to the first afternoon bar.

basicd.calendar.endtimes cannot be used here (its calendar store is not
available on the dev box and returns an empty grid), so the grid is
generated in-package and pinned by tests.
"""

from bisect import bisect_right
from dataclasses import dataclass

NS = 1_000_000_000
MIN_NS = 60 * NS
THREE_MIN_NS = 3 * MIN_NS


def ns_of(h: int, m: int, s: int = 0, ms: int = 0) -> int:
    """Nanoseconds past midnight for a wall-clock time."""
    return ((h * 60 + m) * 60 + s) * NS + ms * 1_000_000


def endtimes_3m_cn() -> list[int]:
    """The 81 CN 3m endtimes, in ns past midnight, ascending."""
    morning = [ns_of(9, 30) + i * THREE_MIN_NS for i in range(41)]  # 09:30..11:30
    assert morning[-1] == ns_of(11, 30)
    afternoon = [ns_of(13, 3) + i * THREE_MIN_NS for i in range(39)]  # 13:03..14:57
    assert afternoon[-1] == ns_of(14, 57)
    return morning + afternoon + [ns_of(15, 0)]


@dataclass(frozen=True)
class Grid:
    endtimes: tuple[int, ...]

    def window_index(self, t_ns: int) -> int:
        """First window whose endtime is strictly greater than the tick,
        clamped into the final (15:00) bar — verbatim `BarGrid::window_index`."""
        i = bisect_right(self.endtimes, t_ns)
        return min(i, len(self.endtimes) - 1)

    def last_endtime_ns(self) -> int:
        return self.endtimes[-1]


def make_grid(freq_min: int = 3) -> Grid:
    if freq_min != 3:
        raise ValueError(f"only freq=3m supported for now, got {freq_min}m")
    return Grid(endtimes=tuple(endtimes_3m_cn()))
