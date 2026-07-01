# lab1 MapReduce
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

