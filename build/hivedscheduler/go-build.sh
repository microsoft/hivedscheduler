#!/bin/bash

# MIT License
#
# Copyright (c) Microsoft Corporation. All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE

set -o errexit
set -o nounset
set -o pipefail

TEST_FLAG=${1:-}
BASH_DIR=$(cd $(dirname ${BASH_SOURCE}) && pwd)
# Ensure ${PROJECT_DIR} is ${GOPATH}/src/github.com/microsoft/hivedscheduler
PROJECT_DIR=${BASH_DIR}/../..
DIST_DIR=${PROJECT_DIR}/dist/hivedscheduler

cd ${PROJECT_DIR}

rm -rf ${DIST_DIR}
mkdir -p ${DIST_DIR}

go build -o ${DIST_DIR}/hivedscheduler cmd/hivedscheduler/*
if [ ! -z ${TEST_FLAG} ] && [ ${TEST_FLAG} == "test" ]; then
  go test ./...
fi
chmod a+x ${DIST_DIR}/hivedscheduler
cp -r bin/hivedscheduler/* ${DIST_DIR}
cp -r example/config/default/hivedscheduler.yaml ${DIST_DIR}

echo Succeeded to build binary distribution into ${DIST_DIR}:
cd ${DIST_DIR} && ls -lR .
