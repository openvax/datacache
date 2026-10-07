"""Run documentation examples that show their output, and compare it.

A python block followed by a text block, with only blank lines between them,
is an example with output; other text blocks, such as signatures, are not.
Each page's examples share one namespace, in order.
"""

import contextlib
import io
import re
import sys
from pathlib import Path

# Document the checked-out code, not whichever datacache is installed.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def main():
    count = 0
    for path in sorted(Path("docs").rglob("*.md")):
        namespace = {"__name__": "docs_example"}
        text = path.read_text()
        blocks = list(re.finditer(r"```(python|text)\n(.*?)\n```", text, re.S))
        for block, following in zip(blocks, blocks[1:]):
            adjacent = not text[block.end():following.start()].strip()
            if block[1] != "python" or following[1] != "text" or not adjacent:
                continue
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                exec(compile(block[2], str(path), "exec"), namespace)
            if output.getvalue().rstrip() != following[2].rstrip():
                raise AssertionError(
                    f"{path}: {output.getvalue().rstrip()!r} != {following[2].rstrip()!r}")
            count += 1
    print(f"Executed {count} documentation examples.")


if __name__ == "__main__":
    main()
