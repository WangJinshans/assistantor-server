kafka:
  brokerUrls:
    - "kafka:9093"
  normalTopic: "test"
  errorTopic: "error"
  clientName: "gateway"

  # plaintext, ssl, sasl_plaintext, sasl_ssl
  securityProtocol: "ssl"

redis:
  host: "redis"
  port: 6379
  password: "root"
  db: 0

maxQps: 10000
maxConnection: 1000
# timeout for socket read/write
socketTimeout: 60

# debug, info, error, fatal
logLevel: info
enableLogToFile: false // 日志到文件
enableLogLine: false

enableChannelProducer: true

enableConnReport: true

protocol: gb17691
