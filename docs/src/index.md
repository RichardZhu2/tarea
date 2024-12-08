---
title: Home
description: "Concurrent Python made simple"
layout: home
nav_order: 1
permalink: /
---

<p align="center">
  <img src="https://raw.githubusercontent.com/pyper-dev/pyper/refs/heads/main/docs/src/assets/img/pyper.png" alt="Pyper" style="width: 500px;">
</p>
<p align="center" style="font-size: 1.5em;">
    <em>Concurrent Python made simple</em>
</p>

<p align="center">
<a href="https://github.com/pyper-dev/pyper/actions/workflows/test.yml" target="_blank">
    <img src="https://github.com/pyper-dev/pyper/actions/workflows/test.yml/badge.svg" alt="Test">
</a>
<a href="https://coveralls.io/github/pyper-dev/pyper" target="_blank">
    <img src="https://coveralls.io/repos/github/pyper-dev/pyper/badge.svg" alt="Coverage">
</a>
<a href="https://pypi.org/project/python-pyper" target="_blank">
    <img src="https://img.shields.io/pypi/v/python-pyper?color=%2334D058&label=pypi%20package" alt="Package version">
</a>
<a href="https://pypi.org/project/python-pyper" target="_blank">
    <img src="https://img.shields.io/pypi/pyversions/python-pyper.svg?color=%2334D058" alt="Supported Python versions">
</a>
</p>

---

## Introduction

Concurrency and parallelism are really hard to get right.

The Python package space has great support for achieving

* concurrency in web applications (Django, Flask, FastAPI, etc.)
* parallelism for distributed computing (Ray, Dask, etc.)

However, the solutions for _general-purpose_ data processing are less established.

Pyper aims to offer a comprehensive framework for concurrent and parallel data-processing in Python, designed with the following goals in mind:

* **Unified API**: Combine threads, processes and async code using one intuitive pattern
* **Functional Paradigm**: Data pipelines compose together flexibly as functions
* **Lazy Execution**: Built from the ground up to support generators, and provides mechanisms for fine-grained memory control
* **Error Handling**: Data flows fail fast, even in long-running threads, and propagate their errors cleanly
* **Complex Data Flows**: Data pipelines support branching/joining data flows, as well as sharing contexts/resources between tasks

In addition, Pyper provides an extensible way to write code that can be integrated with other frameworks like those aforementioned.

## Installation

Install the latest version using `pip`:

```console
$ pip install python-pyper
```

## Where Next?

* Check out the 📖 **[User Guide](./docs/UserGuide/)** to get started with Pyper

* See some 🎯 **[Examples](./docs/Examples/)** of possible use cases
