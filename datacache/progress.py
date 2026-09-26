"""Optional progress displays. Importing datacache never imports tqdm."""


class Progress:
    """Own one tqdm bar for one operation or download attempt."""

    def __init__(self, enabled=False, description="", total=None, unit="B"):
        self.enabled = enabled
        self.description = description
        self.total = total
        self.unit = unit
        self.bar = None

    def __enter__(self):
        if self.enabled:
            try:
                from tqdm.auto import tqdm
            except ImportError as error:
                raise ImportError(
                    "Progress displays require tqdm; install 'datacache[progress]' "
                    "or use show_progress=False") from error
            self.bar = tqdm(
                desc=self.description, total=self.total, unit=self.unit,
                unit_scale=self.unit == "B", unit_divisor=1024,
                dynamic_ncols=True, leave=False)
        return self

    def __call__(self, completed, total):
        if self.bar is not None:
            if completed < self.bar.n:
                self.bar.reset(total=total)
            self.bar.total = total
            self.bar.update(completed - self.bar.n)

    def __exit__(self, *exc):
        if self.bar is not None:
            self.bar.close()
