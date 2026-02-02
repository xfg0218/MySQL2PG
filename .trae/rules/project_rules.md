# 标准项目结构
mysql2pg/
├── cmd/                          # 应用程序入口
├── internal/                     # 转换代码
│   ├── config/                   # 配置管理
│   ├── converter/                # 数据转换核心
│   │   ├── postgres/             # PostgreSQL 连接
│   │   └── greenplum/            # Greenplum 连接
│   ├── mysql/                    # mysql 连接
│   ├── postgres/                 # PostgreSQL 连接
├── scripts/                      # 脚本文件
│   ├── integration/              # 集成测试
│   ├── mysql/                    # MySQL 测试脚本
│   ├── postgres/                 # PostgreSQL 验证脚本
├── test/                         # 测试相关

# 代码格式化
gofmt -w *.go

# 导入顺序
import (​
    "fmt"          // 标准库​
    "net/http"​
    "github.com/gin-gonic/gin"  // 第三方包​
    "myproject/internal/user"   // 本地包​
)

# 代码规范
- 读取 MySQL 代码需要兼容 MySQL 5.x 和 MySQL 8.x 和 MySQL 9.x 的语法
- 需要兼容 PostgreSQL 12.x 版本以上的语法
- 代码需要深度分析，并给出每一步的分析的过程，需要使用正则匹配的方式生成，来适配通用的语法
- 代码注释需要使用中文
