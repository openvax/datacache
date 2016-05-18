# Copyright (c) 2014. Mount Sinai School of Medicine
#
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

from setuptools import setup

readme_filename = "README.md"
current_directory = os.path.dirname(__file__)
readme_path = os.path.join(current_directory, readme_filename)


try:
    with open(readme_path, "r") as f:
        readme = f.read()
except:
    logging.warn("Failed to load %s", readme_filename)
    readme = ""

try:
    import pypandoc
    readme = pypandoc.convert(readme, to="rst", format="md")
except:
    logging.warn("Failed to convert %s to reStructuredText", readme_filename)


if __name__ == "__main__":
    setup(
        name="datacache",
        version="0.4.17",
        description="Helpers for transparently downloading datasets",
        author="Alex Rubinsteyn",
        author_email="alex {dot} rubinsteyn {at} mssm {dot} edu",
        url="https://github.com/hammerlab/datacache",
        license="http://www.apache.org/licenses/LICENSE-2.0.html",
        classifiers=[
            "Development Status :: 3 - Alpha",
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
            "biopython>=1.65",
            "requests>=2.5.1",
            "typechecks>=0.0.2",
        ],
        long_description=readme,
        packages=["datacache"],
    )
