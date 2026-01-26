# 标准项目结构
mysql2pg/
├── cmd/                          # 应用程序入口
├── internal/                     # 私有代码
│   ├── config/                   # 配置管理
│   ├── converter/                # 数据转换核心
│   │   ├── postgres/             # PostgreSQL 连接
│   │   └── greenplum/            # Greenplum 连接
│   └── utils/                    # 内部工具函数
├── pkg/                          # 公共库
├── scripts/                      # 脚本文件
├── test/                         # 测试相关
│   ├── integration/              # 集成测试
│   ├── e2e/                      # 端到端测试
│   ├── mock/                     # 模拟数据
│   └── fixtures/                 # 测试数据
├── docs/                         # 文档

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
- 写入 PostgreSQL 代码需要兼容 PostgreSQL 9.x 以上的语法
- 代码需要深度分析，并给出每一步的分析的过程，需要使用正则匹配的方式生成，来适配通用的语法
- 代码注释需要使用中文
