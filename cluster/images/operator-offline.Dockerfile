FROM golang:alpine AS builder

LABEL stage=gobuilder

ENV CGO_ENABLED 0
ENV GOOS linux

WORKDIR /build

ADD go.mod .
ADD go.sum .
RUN go mod download
COPY operator operator
COPY pkg pkg
RUN go build -ldflags="-s -w" -a -o /app/karmada-operator ./operator/cmd/operator/operator.go


FROM alpine:3.20.2

RUN apk update --no-cache && apk add --no-cache ca-certificates

RUN wget https://github.com/karmada-io/karmada/releases/download/v1.9.6/crds.tar.gz \
     && mkdir -p /var/lib/karmada/1.9.6 && mv crds.tar.gz /var/lib/karmada/1.9.6 \
     && wget https://github.com/karmada-io/karmada/releases/download/v1.7.0/crds.tar.gz \
     && mkdir -p /var/lib/karmada/1.7.0 && mv crds.tar.gz /var/lib/karmada/1.7.0 \
     && wget https://github.com/karmada-io/karmada/releases/download/v1.8.0/crds.tar.gz \
     && mkdir -p /var/lib/karmada/1.8.0 && mv crds.tar.gz /var/lib/karmada/1.8.0

ENV TZ Asia/Shanghai

COPY --from=builder /app/karmada-operator /bin/karmada-operator