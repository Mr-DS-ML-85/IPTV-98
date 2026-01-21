import json
from pathlib import Path

from .config import Time


class Cache:
    now_ts: float = Time.now().timestamp()

    def __init__(self, file: str, exp: int | float) -> None:
        self.file = Path(__file__).parent.parent / "caches" / file

        self.exp = exp

    def is_fresh(self, entry: dict) -> bool:
        ts: float | int = entry.get("timestamp", Time.default_8())

        dt_ts = Time.clean(Time.from_ts(ts)).timestamp()

        return self.now_ts - dt_ts < self.exp

    def write(self, data: dict) -> None:
        self.file.parent.mkdir(parents=True, exist_ok=True)

        self.file.write_text(
            json.dumps(
                data,
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    def load(
        self,
        per_entry: bool = True,
        index: int | None = None,
    ) -> dict[str, dict[str, str | float]]:

        try:
            data: dict = json.loads(self.file.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

        if per_entry:
            return {k: v for k, v in data.items() if self.is_fresh(v)}

        if index:
            ts: float | int = data[index].get("timestamp", Time.default_8())

        else:
            ts: float | int = data.get("timestamp", Time.default_8())

        dt_ts = Time.clean(Time.from_ts(ts)).timestamp()

        return data if self.is_fresh({"timestamp": dt_ts}) else {}


__all__ = ["Cache"]
