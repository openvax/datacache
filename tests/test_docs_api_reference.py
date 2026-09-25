# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Keep docs/api.md true: its version, its coverage, and its runnable examples.

The reference page hardcodes a release version, claims to cover every name in
``datacache.__all__`` plus every public ``Cache`` method, and tells readers to
run its examples in order in one session. Nothing but these checks stops those
promises from drifting away from the code.
"""

import inspect
import re
from pathlib import Path

import pytest

import datacache
from datacache.version import __version__


REFERENCE = Path(__file__).parent.parent / "docs" / "api.md"


def reference_text():
    if not REFERENCE.is_file():
        pytest.skip("docs/api.md is not available in this installation")
    return REFERENCE.read_text()


def canonical_annotations(text):
    """Compare annotations the same way on every supported Python version.

    inspect.signature renders typing.Optional[X] as "Optional[X]" before
    Python 3.14 and as "X | None" from 3.14 on, and qualifies dataclass
    references with their module. Reduce both spellings to one form so the
    reference does not have to name a particular interpreter's formatting.
    """
    text = re.sub(r"\s+", "", text).replace("datacache.inspection.", "")
    while True:
        start = text.find("Optional[")
        if start == -1:
            return text
        opening = start + len("Optional[") - 1
        depth = 0
        for index in range(opening, len(text)):
            if text[index] == "[":
                depth += 1
            elif text[index] == "]":
                depth -= 1
                if depth == 0:
                    inner = text[opening + 1:index]
                    text = text[:start] + inner + "|None" + text[index + 1:]
                    break
        else:
            return text


def test_documented_version_matches_package():
    text = reference_text()
    documented = re.findall(r"in DataCache (\d+\.\d+\.\d+)\.", text)
    assert documented == [__version__], (
        "docs/api.md names DataCache %s but datacache.version.__version__ is %s; "
        "update the reference when bumping the version." % (documented, __version__))


def test_every_public_name_is_documented():
    text = reference_text()
    headings = set(re.findall(r"^#+\s+`?([A-Za-z_][\w.]*)", text, re.M))
    missing = sorted(name for name in datacache.__all__ if name not in headings)
    assert not missing, "docs/api.md has no section for: %s" % missing
    methods = sorted(
        name for name in vars(datacache.Cache)
        if not name.startswith("_") and callable(getattr(datacache.Cache, name)))
    undocumented = sorted("Cache.%s" % name for name in methods
                          if "Cache.%s" % name not in headings)
    assert not undocumented, "docs/api.md has no section for: %s" % undocumented


def test_documented_signatures_match_code():
    text = reference_text()
    mismatched = []
    for block in re.findall(r"```text\n(.*?)```", text, re.S):
        collapsed = " ".join(block.split())
        match = re.match(r"^([A-Za-z_][\w.]*)\((.*)\)$", collapsed)
        if match is None:
            continue
        name, documented = match.group(1), match.group(2)
        if name.startswith("Cache."):
            signature = inspect.signature(getattr(datacache.Cache, name.split(".", 1)[1]))
            parameters = list(signature.parameters.values())[1:]
        else:
            signature = inspect.signature(getattr(datacache, name))
            parameters = list(signature.parameters.values())
        actual = str(signature.replace(
            parameters=parameters, return_annotation=inspect.Signature.empty))[1:-1]
        if canonical_annotations(actual) != canonical_annotations(documented):
            mismatched.append("%s\n  documented: %s\n  actual:     %s" % (name, documented, actual))
    assert not mismatched, "docs/api.md signatures are stale:\n" + "\n".join(mismatched)


def test_examples_run_in_order():
    """The page's examples share one namespace and must run offline, in order."""
    pytest.importorskip("pandas")
    blocks = re.findall(r"```python\n(.*?)```", reference_text(), re.S)
    assert len(blocks) > 1, "docs/api.md should contain runnable examples"
    namespace = {"__name__": "docs_api_examples"}
    try:
        for index, block in enumerate(blocks):
            source = compile(block, "docs/api.md[block %d]" % index, "exec")
            exec(source, namespace)
    finally:
        # The page cleans up in its last block; do it again if a block failed.
        temporary = namespace.get("temporary")
        if temporary is not None:
            temporary.cleanup()
