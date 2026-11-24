# Releasing Datacache

This document explains what to do once your [Pull Request](https://www.atlassian.com/git/tutorials/making-a-pull-request/) has been reviewed and all final changes applied. Now you're ready to merge your branch into master and release it to the world:

1. Bump the [version](http://semver.org/) in `datacache/version.py`, as part of the PR you want to release,
   and add a `CHANGELOG.md` section for it.
   `tests/test_docs_api_reference.py` verifies that every public export in `datacache.__all__` is listed in
   `docs/api.md` and that the API reference examples stay runnable.
2. Merge your branch into master.
3. From a clean `master`, run `./deploy.sh`. It lint-checks, runs the tests, builds the
   `sdist`/`wheel` with [`build`](https://pypi.org/project/build/), and uploads them to
   PyPI with [`twine`](https://pypi.org/project/twine/). You'll need PyPI upload
   credentials configured (e.g. in `~/.pypirc` or via `TWINE_USERNAME`/`TWINE_PASSWORD`).
