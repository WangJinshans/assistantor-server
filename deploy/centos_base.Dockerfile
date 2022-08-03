
FROM centos:7
RUN yum update -y

RUN yum install -y build-base git bash pkgconfig tzdata && cp -r -f /usr/share/zoneinfo/Hongkong /etc/localtime
RUN yum install -y openssl openssl-dev cyrus-sasl-dev lz4-dev wget

RUN wget https://golang.org/dl/go1.16.4.linux-amd64.tar.gz
RUN rm -rf /usr/local/go && tar -C /usr/local -xzf go1.16.4.linux-amd64.tar.gz
RUN echo 'export GOPATH=/usr/local/go' >> /etc/profile
RUN echo 'export GOBIN=$GOPATH/bin' >> /etc/profile
RUN echo 'export PATH=$PATH:$GOBIN' >> /etc/profile
RUN source /etc/profile && go env -w GO111MODULE=on && go env -w GOPROXY=https://goproxy.cn,direct   # 保持会话

#RUN echo 'export GOPATH=/usr/local/go' >> ~/.bash_profile
#RUN echo 'export GOBIN=$GOPATH/bin' >> ~/.bash_profile
#RUN echo 'export PATH=$PATH:$GOBIN' >> ~/.bash_profile
#RUN source ~/.bash_profile

#RUN yum install -y epel-release
#RUN yum install -y golang
RUN yum install -y librdkafka-devel



# docker build -t golang1.16 -f ./deploy/base.Dockerfile .