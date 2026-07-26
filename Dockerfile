FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY main.go .

# Static Linux binary with stripped debug symbols.
RUN CGO_ENABLED=0 GOOS=linux go build -buildvcs=false -ldflags="-s -w" -o server main.go

FROM alpine:3.19

RUN apk add --no-cache ca-certificates

RUN addgroup -S app && adduser -S app -G app
WORKDIR /app

COPY --from=builder /app/server .
COPY portfolio.html .

RUN chown -R app:app /app
USER app

EXPOSE 8080

CMD ["./server"]
