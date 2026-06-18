# Releasing Datacache

This document explains what to do once your [Pull Request](https://www.atlassian.com/git/tutorials/making-a-pull-request/) has been reviewed and all final changes applied. Now you're ready to merge your branch into master and release it to the world:

1. Bump the [version](http://semver.org/) in `datacache/version.py`, as part of the PR you want to release.
2. Merge your branch into master.
3. From a clean `master`, run `./deploy.sh`. It lint-checks, runs the tests, builds the
   `sdist`/`wheel` with [`build`](https://pypi.org/project/build/), and uploads them to
   PyPI with [`twine`](https://pypi.org/project/twine/). You'll need PyPI upload
   credentials configured (e.g. in `~/.pypirc` or via `TWINE_USERNAME`/`TWINE_PASSWORD`).

> **Note:** `python setup.py sdist upload` is deprecated and no longer works — PyPI
> rejects uploads from `setup.py`, and the project has moved to `pyproject.toml` (there
> is no `setup.py`). Always release via `./deploy.sh` (i.e. `build` + `twine`).
