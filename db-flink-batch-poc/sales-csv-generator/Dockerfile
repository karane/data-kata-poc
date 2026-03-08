FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod ./
COPY *.go .
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o generator .

FROM alpine:3.19

WORKDIR /app
COPY --from=builder /app/generator .

ENTRYPOINT ["./generator"]
