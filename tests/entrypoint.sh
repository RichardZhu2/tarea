#!/bin/bash
set -e
exec tox -e "${PYTHON_VERSION}"
