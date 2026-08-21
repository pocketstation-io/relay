FROM golang:1.26-alpine AS builder
ARG GIT_SHA=unknown
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o relay ./cmd/relay-server/

FROM gcr.io/distroless/static:nonroot
ARG GIT_SHA=unknown
LABEL org.opencontainers.image.revision=$GIT_SHA
COPY --from=builder /app/relay /relay
EXPOSE 8080 8081/tcp 3478/udp 3478/tcp
ENTRYPOINT ["/relay"]
