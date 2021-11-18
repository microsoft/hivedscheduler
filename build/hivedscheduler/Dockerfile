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

FROM golang:1.13.15-alpine as builder

ARG TEST=false
ENV PROJECT_DIR=${GOPATH}/src/github.com/microsoft/hivedscheduler
ENV INSTALL_DIR=/opt/hivedscheduler/hivedscheduler

RUN apk update && apk add --no-cache bash g++ && \
  mkdir -p ${PROJECT_DIR} ${INSTALL_DIR}
COPY . ${PROJECT_DIR}
RUN if [ ${TEST} == "true" ]; \
  then ${PROJECT_DIR}/build/hivedscheduler/go-build.sh test; \
  else ${PROJECT_DIR}/build/hivedscheduler/go-build.sh; fi && \
  mv ${PROJECT_DIR}/dist/hivedscheduler/* ${INSTALL_DIR}


FROM alpine:3.10.1

ENV INSTALL_DIR=/opt/hivedscheduler/hivedscheduler

RUN apk update && apk add --no-cache bash
COPY --from=builder ${INSTALL_DIR} ${INSTALL_DIR}
WORKDIR ${INSTALL_DIR}

ENTRYPOINT ["./start.sh"]
