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

# 开发环境
1. 请保持代码的可读性和可维护性，并且注释为中文
2. 请在生成代码时添加函数级注释
3. 保持代码整洁，避免重复代码和冗长的函数，每个函数只负责一个具体的任务，避免复杂的逻辑嵌套
4. 所有的函数都需要有简短的一句话注释
5. 生成的代码需要适配 MySQL5.7 + 的语法
6. 生成的代码需要兼容 PostgreSQL 12+ 和 Greenplum 6+ 的语法
7. 在现有的代码功能的基础上，不进行修改逻辑的情况下，进行新代码的修改
8. 开发遵循golang的代码规范
9. 新增的功能都需要在sync_***_test.go文件中进行增加对应的测试案例
10. 测试用例的编写需要遵循golang的测试规范，每个测试用例都需要有对应的测试函数，函数名以Test开头，参数为testing
