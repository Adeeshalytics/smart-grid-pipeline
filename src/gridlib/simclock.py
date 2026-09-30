"""Simulated clock."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from .config import Settings


@dataclass(frozen=True)
class SimClock:
    anchor_wall: datetime
    sim_epoch: datetime
    day_seconds: int

    @property
    def speedup(self) -> float:
        return 86400.0 / self.day_seconds

    def to_sim(self, wall: datetime) -> datetime:
        return self.sim_epoch + (wall - self.anchor_wall) * self.speedup

    def to_wall(self, sim: datetime) -> datetime:
        return self.anchor_wall + (sim - self.sim_epoch) / self.speedup

    def now(self) -> datetime:
        return self.to_sim(datetime.now(timezone.utc))

    def day_index(self, wall: datetime) -> int:
        """Index of the simulated day that contains the wall-clock instant."""
        return int((wall - self.anchor_wall).total_seconds() // self.day_seconds)

    def sim_date(self, index: int) -> date:
        return (self.sim_epoch + timedelta(days=index)).date()

    def day_end_wall(self, index: int) -> datetime:
        """Wall-clock instant at which simulated day `index` ends."""
        return self.anchor_wall + timedelta(seconds=(index + 1) * self.day_seconds)

    def wall_seconds(self, sim_delta: timedelta) -> float:
        """Convert a simulated duration into wall-clock seconds."""
        return sim_delta.total_seconds() / self.speedup


def floor_to_day_boundary(wall: datetime, day_seconds: int) -> datetime:
    """Align the anchor to a multiple of day_seconds since the Unix epoch."""
    epoch = int(wall.timestamp())
    return datetime.fromtimestamp(epoch - epoch % day_seconds, tz=timezone.utc)


def load_clock(settings: Settings, conn) -> SimClock:
    """Read the shared clock from PostgreSQL, creating it on first use."""
    anchor = floor_to_day_boundary(datetime.now(timezone.utc), settings.sim_day_seconds)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sim_clock (id, anchor_wall, sim_epoch, day_seconds)
            VALUES (1, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            (anchor, settings.sim_epoch, settings.sim_day_seconds),
        )
        cur.execute("SELECT anchor_wall, sim_epoch, day_seconds FROM sim_clock WHERE id = 1")
        anchor_wall, sim_epoch, day_seconds = cur.fetchone()
    conn.commit()
    return SimClock(anchor_wall=anchor_wall, sim_epoch=sim_epoch, day_seconds=day_seconds)
