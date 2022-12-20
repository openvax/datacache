# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import logging
import re

from setuptools import setup

readme_filename = "README.md"
current_directory = os.path.dirname(__file__)
readme_path = os.path.join(current_directory, readme_filename)
project_name = "datacache"

try:
    with open(readme_path, "r") as f:
        readme_markdown = f.read()
except:
    logging.warn("Failed to load %s", readme_filename)
    readme_markdown = ""

with open('%s/version.py' % project_name, 'r') as f:
    version = re.search(
        r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
        f.read(),
        re.MULTILINE).group(1)

if __name__ == "__main__":
    setup(
        name=project_name,
        version=version,
        description="Helpers for transparently downloading datasets",
        author="Alex Rubinsteyn",
        author_email="alex.rubinsteyn@unc.edu",
        url="https://github.com/openvax/%s" % project_name,
        license="http://www.apache.org/licenses/LICENSE-2.0.html",
        classifiers=[
            "Development Status :: 4 - Beta",
            "Environment :: Console",
            "Operating System :: OS Independent",
            "Intended Audience :: Science/Research",
            "License :: OSI Approved :: Apache Software License",
            "Programming Language :: Python",
            "Topic :: Scientific/Engineering :: Bio-Informatics",
        ],
        install_requires=[
            "pandas>=0.15.2",
            "appdirs>=1.4.0",
            "progressbar33>=2.4",
            "requests>=2.5.1",
            "typechecks>=0.0.2",
            "mock",
        ],
        long_description=readme_markdown,
        long_description_content_type='text/markdown',
        packages=[project_name],
    )
