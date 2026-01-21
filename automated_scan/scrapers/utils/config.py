import json
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytz


class Time(datetime):
    ZONES = {
        "CET": pytz.timezone("Europe/Berlin"),
        "ET": pytz.timezone("America/New_York"),
        "PST": pytz.timezone("America/Los_Angeles"),
        "UTC": timezone.utc,
    }

    ZONES["EDT"] = ZONES["EST"] = ZONES["ET"]

    TZ = ZONES["ET"]

    @classmethod
    def now(cls) -> "Time":
        return cls.from_ts(datetime.now(cls.TZ).timestamp())

    @classmethod
    def from_ts(cls, ts: int | float) -> "Time":
        return cls.fromtimestamp(ts, tz=cls.TZ)

    @classmethod
    def default_8(cls) -> float:
        return (
            cls.now()
            .replace(hour=8, minute=0, second=0, microsecond=0, tzinfo=cls.TZ)
            .timestamp()
        )

    def delta(self, **kwargs) -> "Time":
        return self.from_ts((self + timedelta(**kwargs)).timestamp())

    def clean(self) -> "Time":
        return self.__class__.fromtimestamp(
            self.replace(second=0, microsecond=0).timestamp(),
            tz=self.TZ,
        )

    def to_tz(self, tzone: str) -> "Time":
        dt = self.astimezone(self.ZONES[tzone])

        return self.__class__.fromtimestamp(dt.timestamp(), tz=self.ZONES[tzone])

    @classmethod
    def _to_class_tz(cls, dt) -> "Time":
        dt = dt.astimezone(cls.TZ)

        return cls.fromtimestamp(dt.timestamp(), tz=cls.TZ)

    @classmethod
    def from_only_time(cls, s: str, d: date, timezone: str) -> "Time":
        hour, minute = map(int, s.split(":"))

        dt = datetime(
            2000,
            1,
            1,
            hour,
            minute,
            tzinfo=cls.ZONES.get(timezone, cls.TZ),
        )

        dt = dt.astimezone(cls.TZ)

        dt = datetime.combine(d, dt.timetz())

        return cls.fromtimestamp(dt.timestamp(), tz=cls.TZ)

    @classmethod
    def from_str(
        cls,
        s: str,
        fmt: str | None = None,
        timezone: str | None = None,
    ) -> "Time":
        tz = cls.ZONES.get(timezone, cls.TZ)

        if fmt:
            dt = datetime.strptime(s, fmt)

            dt = tz.localize(dt)

        else:
            formats = [
                "%b %d, %Y %H:%M %Z",
                "%B %d, %Y %H:%M",
                "%B %d, %Y %I:%M %p",
                "%B %d, %Y %I:%M:%S %p",
                "%B %d, %Y %H:%M:%S",
                "%Y-%m-%d",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %I:%M %p",
                "%Y-%m-%d %H:%M %p",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%fZ",
                "%Y/%m/%d %H:%M",
                "%Y/%m/%d %H:%M:%S",
                "%Y/%m/%dT%H:%M:%S.%fZ",
                "%m/%d/%Y %H:%M",
                "%m/%d/%Y %I:%M %p",
                "%m/%d/%Y %H:%M:%S",
                "%a, %d %b %Y %H:%M:%S %z",
            ]

            for frmt in formats:
                try:
                    dt = datetime.strptime(s, frmt)
                    break
                except ValueError:
                    continue
            else:
                return cls.from_ts(Time.default_8())

            if not dt.tzinfo:
                dt = (
                    tz.localize(dt)
                    if hasattr(tz, "localize")
                    else dt.replace(tzinfo=tz)
                )

        return cls._to_class_tz(dt)


class Leagues:
    live_img = "https://i.gyazo.com/4a5e9fa2525808ee4b65002b56d3450e.png"

    def __init__(self) -> None:
        self.data = json.loads(
            (Path(__file__).parent / "leagues.json").read_text(encoding="utf-8")
        )

    def teams(self, league: str) -> list[str]:
        return self.data["teams"].get(league, [])

    def info(self, name: str) -> tuple[str | None, str]:
        name = name.upper()

        if match := next(
            (
                (tvg_id, league_data.get("logo"))
                for tvg_id, leagues in self.data["leagues"].items()
                for league_entry in leagues
                for league_name, league_data in league_entry.items()
                if name == league_name or name in league_data.get("names", [])
            ),
            None,
        ):
            tvg_id, logo = match

            return (tvg_id, logo or self.live_img)

        return (None, self.live_img)

    def is_valid(
        self,
        event: str,
        league: str,
    ) -> bool:

        pattern = re.compile(r"\s+(?:-|vs\.?|at|@)\s+", re.IGNORECASE)

        if pattern.search(event):
            t1, t2 = re.split(pattern, event)

            return any(t in self.teams(league) for t in (t1.strip(), t2.strip()))

        return event.lower() in {
            "nfl redzone",
            "redzone",
            "red zone",
            "college gameday",
        }

    def get_tvg_info(
        self,
        sport: str,
        event: str,
    ) -> tuple[str | None, str]:

        match sport:
            case "American Football" | "NFL":
                return (
                    self.info("NFL")
                    if self.is_valid(event, "NFL")
                    else self.info("NCAA")
                )

            case "Basketball" | "NBA":
                if self.is_valid(event, "NBA"):
                    return self.info("NBA")

                elif self.is_valid(event, "WNBA"):
                    return self.info("WNBA")

                else:
                    return self.info("Basketball")

            case "Ice Hockey" | "Hockey":
                return self.info("NHL")

            case _:
                return self.info(sport)


leagues = Leagues()

__all__ = ["leagues", "Time"]
