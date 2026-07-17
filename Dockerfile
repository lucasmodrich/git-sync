FROM golang:1.25-alpine AS builder

WORKDIR /go/src/app

RUN apk add --no-cache upx ca-certificates tzdata git

ARG VERSION=main
ARG BUILD="N/A"

ENV GO111MODULE=on \
  CGO_ENABLED=0 \
  GOOS=linux

COPY go.mod go.sum /go/src/app/

RUN go mod download \
  && go mod tidy

COPY . /go/src/app/

RUN go build -a -installsuffix cgo -ldflags="-w -s -X github.com/lucasmodrich/git-sync/pkg/version.Version=${VERSION} -X github.com/lucasmodrich/git-sync/pkg/version.Build=${BUILD}" -o git-sync . \
  && upx -q git-sync

# Application image
FROM alpine:latest
WORKDIR /opt/go

LABEL maintainer="Lucas Modrich"

# Install git since it's required for the application.
# tini is the container's PID 1 and reaps orphaned/zombie processes — without it,
# grandchildren git subprocesses spawn (ssh, git-remote-https, pack-objects) that
# outlive a killed or timed-out git parent would never be reaped, slowly
# exhausting the container's process table over time.
RUN apk add --no-cache git su-exec tini

RUN mkdir -p /git-sync /backups

COPY --from=builder /go/src/app/git-sync /opt/go/git-sync
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/sbin/tini", "--", "/entrypoint.sh", "/opt/go/git-sync"]
CMD ["--config", "/git-sync/config.yaml", "--backup-dir", "/backups"]
