FROM golang1.16


WORKDIR /build

COPY . .

RUN source /etc/profile
RUN go mod tidy
RUN go build -o message-server cmd/inline_server/main.go

#CMD ["./message-server"]