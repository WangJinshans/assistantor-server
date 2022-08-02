# Gateway

A compact gateway written in golang, dedicated to data reception and data validation with high performance and high availability.

## Features

- 协议切换
- 数据接收
- 数据包分割
- 数据包校验
- VIN 合法性校验
- 数据包发送到 kafka 消息队列
- 下行指令的拉取与下发
- 连接状态的汇报（由 connection-manager 组件提供接口）
- 可配置的服务的实时监控，暴露 prometheus 标准接口
- 可配置的实时性能报告
- 第三方平台接入
- 快速回复（在网关直接生成回复数据包）
- 存活状态汇报（汇报本实例的存活状态）
- consul 配置读取（可关闭）
- consul 服务注册与服务发现（可关闭）
- yaml 配置文件读取（可关闭）

## Supported Protocol

- gb32960
- gb17691
- hj

## Development Workflow

### Init config file

`make init`

### Install dependencies

`make install`

### Test

`make test`

### Run

`make run`

### Build binary

`make build`

This command will build a binary file in current directory named 'main'

### Build docker image

`make build-image`

### Build minimal image

`make build-image-min`

The image size is as small as less than 20M

### Push image to private repo

`make push`

### Monitoring during development

1. `make stack`
1. Open [Dashboard](http://localhost:3000/d/83pHflXmz/gateway?orgId=1&refresh=5s) in browser:
 ![Dashboard](/docs/grafana.png)

# Setup librdkafka

- https://github.com/edenhill/librdkafka/issues/1896#issuecomment-407468773
- https://github.com/edenhill/librdkafka/wiki/Using-SSL-with-librdkafka

# Core Dump

- [How to get core file of segmentation fault process in Docker - DEV Community 👩‍💻👨‍💻](https://dev.to/mizutani/how-to-get-core-file-of-segmentation-fault-process-in-docker-22ii)
