FROM golang:1.20


ENV GO111MODULE=on

WORKDIR /app
COPY . .

RUN go build -o /app/notifier


# Run
CMD ["/app/notifier"]