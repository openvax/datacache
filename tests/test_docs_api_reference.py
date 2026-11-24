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

"""Keep docs/api.md coverage and docs/reference/examples.md runnable examples in sync."""

import re
from pathlib import Path

import pytest

import datacache


REFERENCE = Path(__file__).parent.parent / "docs" / "api.md"
EXAMPLES = Path(__file__).parent.parent / "docs" / "reference" / "examples.md"


def read_doc(path):
    if not path.is_file():
        pytest.skip("%s is not available in this installation" % path)
    return path.read_text()


def test_every_public_name_is_documented():
    text = read_doc(REFERENCE)
    directives = set(re.findall(r"^:::\s+datacache\.([A-Za-z_][\w]*)\s*$", text, re.M))
    missing = sorted(name for name in datacache.__all__ if name not in directives)
    assert not missing, "docs/api.md has no mkdocstrings directive for: %s" % missing


def test_examples_run_in_order():
    """The API examples share one namespace and must run offline, in order."""
    pytest.importorskip("pandas")
    blocks = re.findall(r"```python\n(.*?)```", read_doc(EXAMPLES), re.S)
    assert len(blocks) > 1, "docs/reference/examples.md should contain runnable examples"
    namespace = {"__name__": "docs_api_examples"}
    try:
        for index, block in enumerate(blocks):
            source = compile(block, "docs/reference/examples.md[block %d]" % index, "exec")
            exec(source, namespace)
    finally:
        # The page cleans up in its last block; do it again if a block failed.
        temporary = namespace.get("temporary")
        if temporary is not None:
            temporary.cleanup()
