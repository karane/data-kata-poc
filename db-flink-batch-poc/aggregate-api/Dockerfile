FROM golang:1.26.1-alpine AS builder


WORKDIR /app
COPY go.mod ./
COPY *.go .
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o aggregate-api .

FROM alpine:latest

WORKDIR /root/
COPY --from=builder /app/aggregate-api .

CMD ["./aggregate-api"]