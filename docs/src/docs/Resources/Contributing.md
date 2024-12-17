---
title: Contributing
parent: Resources
layout: default
nav_order: 1
permalink: /docs/Resources/Contributing
---

# Contributing
{: .no_toc }

* TOC
{:toc}

We welcome developers to contribute to this project!

This guide will take you through our contribution guidelines and best practices.

The Pyper repository is hosted on [GitHub](https://github.com/pyper-dev/pyper).
We will assume familiarity with Git-based version control and making [pull requests](https://docs.github.com/get-started/exploring-projects-on-github/contributing-to-a-project).

## Typos

All typo fixes are welcome. Feel free to simply open a pull request.

## Bugs and Features

### Issues

We track all ongoing improvements through GitHub Issues. Start by opening a [new issue](https://github.com/pyper-dev/pyper/issues/new/choose) if one doesn't exist already.

After you have created the issue, or you have found another unassigned issue you would like to work on, please:
* Assign yourself to the issue if you are a collaborator
* OR post a comment asking to be assigned to the issue

### Testing

Assuming you have:

* Been assigned to an issue
* Forked and cloned a copy of the repo
* Made your changes to the source code

We'll want to make sure that tests are passing before pushing and merging these changes.
If the changes you've made warrant writing additional tests, please consider doing so.

To set up the testing environment, install the test dependencies (within a virtual environment):

```console
$ pip install -r tests/requirements.txt
```

Test coverage is measured against Python 3.12 -- we are aiming for 100% code coverage at all times.
Use `tox` to run all tests within a 3.12 environment and generate a coverage report:

```console
$ tox -e 3.12
```

Please also make sure that tests pass successfully for all supported versions.
You can do this without configuring additional Python virtual environments by using [Docker](https://docs.docker.com/):

```console
$ cd tests
$ docker-compose up --build --detach
```

You can verify that all tests have passed succesfully if each container exits with a status code of 0, or by inspecting the Docker logs.

### Documentation

If relevant, please update the documentation appropriately. Documentation source files are found at `/docs/src`. These consist of markdown files, served with Jekyll on Github Pages, using the [just-the-docs](https://github.com/just-the-docs/just-the-docs) theme.

If you would like to serve the documentation locally, you will need to:

* Install [Ruby](https://www.ruby-lang.org/en/documentation/installation/)
* Install [bundler](https://bundler.io/) (environment manager for Ruby)

Install dependencies with:

```console
$ cd docs
$ bundle install
```

Then run:

```console
$ bundle exec jekyll serve
```

This serves the documentation site locally at `http://localhost:4000`, where you can inspect your changes.


