FROM golang:1.23 AS build
WORKDIR /src
COPY . .
RUN go mod download && go build -o /out/relay ./cmd/relay-server
FROM gcr.io/distroless/base-debian12
COPY --from=build /out/relay /relay
EXPOSE 8080
CMD ["/relay"]
