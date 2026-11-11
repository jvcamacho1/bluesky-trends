import contextlib
import json
import os
import tempfile


class CursorStore:
    def __init__(self, path: str):
        self.path = path

    def load(self) -> int | None:
        try:
            with open(self.path) as f:
                data = json.load(f)
            seq = data.get("seq")
            return seq if isinstance(seq, int) else None
        except (OSError, json.JSONDecodeError):
            return None

    def save(self, seq: int) -> None:
        directory = os.path.dirname(self.path) or "."
        os.makedirs(directory, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=directory, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump({"seq": seq}, f)
            os.replace(tmp, self.path)
        except OSError:
            with contextlib.suppress(OSError):
                os.unlink(tmp)
            raise
