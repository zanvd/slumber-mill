FROM golang:1.27 AS go

FROM ubuntu:24.04@sha256:534baea6a22c03a63003dbc8dbe78fe34bc0d7e595d9a9dc9834884ff530eb55 AS base-media
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates \
    ffmpeg=7:6.1.1-3ubuntu5 \
    libavcodec60=7:6.1.1-3ubuntu5 \
    libavfilter9=7:6.1.1-3ubuntu5 \
    libavformat60=7:6.1.1-3ubuntu5 \
    libavutil58=7:6.1.1-3ubuntu5 \
    libswresample4=7:6.1.1-3ubuntu5 \
  && rm -rf /var/lib/apt/lists/*

FROM base-media AS dev
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
  && apt-get install -y --no-install-recommends gcc libc6-dev \
  && rm -rf /var/lib/apt/lists/*
COPY --from=go /usr/local/go /usr/local/go
# ubuntu:24.04 ships a user with UID 1000, so whoever holds APP_UID makes way for app.
ARG APP_UID=1000
RUN existing="$(getent passwd "${APP_UID}" | cut -d: -f1)" \
  && if [ -n "${existing}" ]; then userdel --remove "${existing}"; fi \
  && useradd --uid "${APP_UID}" --user-group --create-home app
ENV GOPATH=/home/app/go \
  GOCACHE=/home/app/.cache/go-build \
  GOTOOLCHAIN=local \
  PATH=/home/app/go/bin:/usr/local/go/bin:${PATH}
USER app
# Named volumes mounted here copy this ownership.
RUN mkdir -p "${GOPATH}/pkg/mod" "${GOCACHE}"
WORKDIR /src

FROM dev AS build
USER root
COPY src/go.mod src/go.sum ./
RUN go mod download
COPY src/ ./
RUN CGO_ENABLED=0 go build -trimpath -o /slumber-mill ./cmd/slumber-mill

FROM gcr.io/distroless/static:nonroot@sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3 AS runtime
ARG APP_UID=1000
COPY --from=build /slumber-mill /usr/local/bin/slumber-mill
USER ${APP_UID}
ENTRYPOINT ["slumber-mill"]

FROM base-media AS runtime-media
ARG APP_UID=1000
RUN existing="$(getent passwd "${APP_UID}" | cut -d: -f1)" \
  && if [ -n "${existing}" ]; then userdel --remove "${existing}"; fi \
  && useradd --uid "${APP_UID}" --user-group --create-home app
COPY --from=build /slumber-mill /usr/local/bin/slumber-mill
USER app
ENTRYPOINT ["slumber-mill"]
