
FROM golang:1.16.1-alpine

RUN apk add build-base git bash pkgconfig tzdata && cp -r -f /usr/share/zoneinfo/Hongkong /etc/localtime
RUN apk add --update --no-cache git bash build-base \
    openssl-dev cyrus-sasl-dev lz4-dev

RUN cd / && git clone -b v1.1.0 https://github.com/edenhill/librdkafka.git && cd librdkafka && ./configure && make && make install

RUN go env -w GOPROXY=https://goproxy.cn,direct
RUN go env -w GO111MODULE=on


# docker build -t golang1.16 -f ./deploy/base.Dockerfile .