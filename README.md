# SpiderProxyHa

这是一个面向爬虫（抓取）业务的代理IP中间件，使用他可以让你采购的代理ip不会出现离线或者不可用的情况。


## 核心功能

#### 1.确定的接入入口
不再像代理云这类ip供应商，每次提供一个ip列表，而且确定的ip：port进行代理访问。对于群控场景，app/网页启动的时候获取ip将会有比较大的成本的

#### 2. 高可用保证
ip列表方式，代理ip过期时间不准确、代理ip出现再列表中但是实际上无法连接、代理ipTTL高无法快速探测，代理突然中断无法感知。

SpiderProxyHa通过tcp层面连接会话保持，可以再毫秒级时间内感知代理服务器掉线。并且再代理服务器掉线之后立即切换到其他可以代理ip源上

所以 SpiderProxyHa上面提供的所有代理一定是永远可以使用的

#### 3. 高性能低配置要求
不用说了，全程NIO，最低配学生服务器都可以跑的很完美

#### 4. 请求加速
SpiderProxyHa虽然中间增加了中间层，但是实际上的代理访问速度不会变慢，甚至有较好的调优之后会变得更快。因为对于所有的代理ip都会提前创建连接。
SpiderProxyHa到上游代理之间会有一层tcp连接池。

SpiderProxyHa将代理请求发出点收拢到固定服务器，并以固定服务器作为基准探测ip ttl。ttl高的代理ip节点则自动排在ip池的尾部，ttl越高的ip被使用到的概率会越低。SpiderProxyHa会优先选择质量好的ip资源

SpiderProxyHa可以探测热点target(Pro版本才会支持)，并且提前创建到最终host的连接。这个机制可以使得SpiderProxyHa的速度比原生代理ip更快一些

在抢单场景下，提前连接池功能可以得到加快几十毫秒




## 使用

### 依赖
基于netty，java生态。要求jdk1.8

### 系统配置要求

要求不高，1核2G的Linux服务器即可

### 构建

mac/linux
```
cd spider-ha-proxy-core & sh mvnw -Pprod  clean -Dmaven.test.skip=true package appassembler:assemble
```
windows
```
cd spider-ha-proxy-core & mvnw.cmd -Pprod  clean -Dmaven.test.skip=true package appassembler:assemble
```

之后得到文件夹:``spider-ha-proxy-core/target/dist-spider-proxy-ha-1.0`` 即为可执行文件


### 配置

本项目不是代理ip产生服务器，而是对代理ip资源进行高可用的中间层服务。也即他是一个代理ip的代理服务器。
使用当前服务之前，需要配置你采购的代理ip。SpiderProxyHa的主要配置文件在``conf/config.ini`` 配置项如下：
```
[global]
type = global
refreshUpstreamInterval = 30
# 连接池中缓存的连接数量
cache_connection_size = 3
# 连接池中缓存的连接时间，太久可能会僵死
cache_connection_seconds = 30
cache_connection_seconds = 30
# lo,private,public,all
# lo: 127.0.0.1 回环地址,适合单机
# private: 内网地址,适合同机房
# public: 外网地址
# all: 监听0.0.0.0，适合在公网
listen_type = lo

# 唯一的名称，可以配置多个采购的代理ip源，或者为业务配置独立的ip源。保证各业务ip使用的资源独立
[source_dly_virjar]
# 配置类型，只能是source
type = source
# 一个代理源的描述
name = 代理云virjar代理源
# 对应的代理支持的协议，目前SpiderProxyHa只支持 http/https/socks4/socks5
# 同时SpiderProxyHa只会做同协议转发，而不会做http over socks(至少开源版本不会做)
protocol = http/https/socks4/socks5

# 非常重要，为你加载ip的数据源，需要是ip:port\nip:port结构，如果不是那么你需要自己实现一个中间服务进行格式转换
# 这里的代理比如代理云或者其他类似的代理ip厂商
source_url = http://dailiyun.v4.dailiyun.com/query.txt?key=WQ5D646245&word=&count=200&rand=true&detail=false
# 本地服务组，SpiderProxyHa会在mapping_space对应的端口上开启代理服务，最终的代理请求则是转发到上游代理资源上
# 需要注意，这里的端口范围不能太大，正常应为source_url返回结果数量的 65%，因为上游加载的ip数量可能有一部分无法使用。另外上游ip出问题的时候，需要留下一定buffer实现软切
mapping_space = 36000-36149

## 注意下，如果你的上游代理ip是通过ip白名单鉴权，或者不需要鉴权，那么这里的密码不需要配置
# 上游代理服务器的账户,你的代理供应商会提供给你
upstream_auth_user = your_proxy_use
# 上游代理服务器的密码,你的代理供应商会提供给你
upstream_auth_password = your_proxy_password

```

### 启动
配置好之后，启动脚本 ``bin/SpiderProxyHa.bat``或者``bin/SpiderProxyHa.sh``

### 业务使用

直接将你业务的代理服务器配置到运行SpiderProxyHa的服务器，端口为 mapping_space配置的端口。

每个端口都会对应一个特定的上游代理ip资源，正常情况下，除非检测到上游ip掉线。否则不会修改mapping关系


### 快速上手

1. 用idea打开本项目
2. 运行upstream-mocker中的UpstreamMocker的main函数(如果是mac系统，可以直接运行脚本:``run_mocker_server.sh``)
3. 在idea maven profile中，选中dev
4. 运行：``spider-ha-proxy-core/src/main/java/com/virjar/spider/proxy/ha/HaProxyBootstrap.java``

