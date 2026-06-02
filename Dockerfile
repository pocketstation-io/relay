FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o relay ./cmd/relay-server/

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /app/relay /relay
EXPOSE 8080 3478/udp
ENTRYPOINT ["/relay"]
