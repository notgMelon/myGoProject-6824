# go init
1.vscode 的go拓展有一部分插件无法下载
更改代理为国内代理，手动使用go install命令下载对应插件，然后重启vscode即可
gopls安装验证：
``` shell
gopls vesion
```
如果出现找不到gopls的情况，说明gopls没有被添加到环境变量中
``` shell
# 检查 gopls 是否已安装到 Go 的 bin 目录
ls $(go env GOPATH)/bin/gopls

# 如果存在，应该能看到 gopls 文件
```
找到gopls文件后，添加到环境变量即可
``` shell
# 临时添加到当前会话
export PATH="$PATH:$(go env GOPATH)/bin"

# 永久添加到 .bashrc
echo 'export PATH="$PATH:'$(go env GOPATH)'/bin"' >> ~/.bashrc
source ~/.bashrc
```
此时再执行验证命令，应该有对应输出，重启vscode后gopls可用。
安装gopls后再类似地使用 go install 安装 staticcheck ,然后重启vscode 

我没有做安装验证，直接重启vscode后拓展提示的staticcheck缺失的消息就不见了，静态检查可用。
另外我在安装了gopls后重启了一次vscode然后再安装staticcheck然后再次重启vscode，没有尝试这两个插件能否一起安装。

# lab1
## 遇到的坑
+ map任务生成的中间文件需要使用json格式编码，不能和reduce的输出一样使用%v格式。json解析高效无歧义，%v格式打印后全都是字符串，难以解决key/value中含有空格、换行的情况。
+ 注意rpc的结构体定义规范，变量首字母要大写
+ worker实现无限循环获取任务，直到done之后结束
+ 无限循环中文件操作的defer close会循环结束后执行；手动close或使用匿名函数
+ job count test会生成误导文件，需要使用正则匹配剔除
## tips
+ 用select case同时监听多个channel，并处理最先收到信息的channel

# lab2
## tips
+ clerk的call的返回值表示是否收到reply
+ 注意6.5840课程的源码、测试方式每年可能有差异，如26年使用make测试，本项目参照的是25年的课程，使用go test测试。
+ (3C) 持久化函数persist()是幂等的，向应用层提交log的动作也是幂等的

# lab3
## 遇到的坑
### 3A
+ 参照3C第一条
+ 仔细查看论文fig 2以透彻理解server在各个状态下的行动逻辑
    + follower变成candidate后立即递增term，不论是否当选；每次启动选举都会递增
    + candidate没有当选应该回退为follower(可以给其他人投票)
    + follower给其他人投票、收到leader rpc都会重置计时器
    + rpc请求超时时间很长，广播**不能**等到全部人都回复后才继续处理
    + 每个服务器每个term一票，不能多投
#### tips
+ ~~ 使用waitgroup批量启动goroutine发送rpc,并等所有routine完成后继续工作 ~~ , 豆包误我TwT
+ 发心跳、拉票请求应该新启动go routine执行，不能阻塞定时器；超时的心跳、拉票应该忽略
### 3B
+ leader收到新log后可以立即发rpc,不需要等待上一个rpc
+ leader要对各follower分别计时维持心跳；rpc失败重试同样重置心跳计时器
+ 同上，长超时的rpc不能阻塞心跳、log发送
+ *重要：*leader启动的对follower发rpc的goroutine等协程要在各回路检查是否越任期工作、已经卸任，避免出现僵尸协程。可能持久化运行的各协程，包括选举worker等非leader启动但同样限定工作任期的协程都需要检查。
    + 这一类协程也需要快照工作状态相关的变量（state, term, ctx等等）
### 3C
+ *重要：*注意需要持久化存储的状态首字母需要大写，其中包括封装结构体内的成员变量，labgob序列化中必需首字母大写
+ leader当选后,如果存在还没有提交的日志（已经多数派达成共识），可以通过提交一条 no‑op（空操作，属于自己任期）日志，间接把之前所有合法日志提交（顺便广播自己为leader）
+ *重要：*对于上层收到的 CommandValid ApplyMsg，测试框架要求 CommandIndex 严格按 1 递增，不能跳号，并且不接受no op为合法操作（操作不能为nil）。本项目选择不实现no op逻辑
+ 参照论文Figure8,leader需要注意不能直接提交旧term的log，只能间接提交