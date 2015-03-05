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


requirements_path = os.path.join(current_directory, "requirements.txt")

with open(requirements_path, "r") as f:
    requirements = [
        line.strip()
        for line in f.read().splitlines()
        if line.strip()
    ]

if __name__ == "__main__":
    setup(
        name="datacache",
        version="0.4.7",
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
        install_requires=requirements,
        long_description=readme,
        packages=["datacache"],
    )
