# 定义Go二进制文件名称
BINARY_NAME=mysql2pg

# 定义源码目录
SRC_DIR=./cmd

# 定义相关文件
CONFIG_FILE=config.yml
ERROR_FILE=errors.log
CONVERSION_FILE=conversion.log

# 构建项目
build:
	@echo "正在构建项目..."
	@go build -o $(BINARY_NAME) $(SRC_DIR)/main.go
	@echo "构建完成！可执行文件: $(BINARY_NAME)"

# 运行项目
run:
	@echo "正在运行项目..."
	@go run $(SRC_DIR)/main.go -c $(CONFIG_FILE)

# 测试数据库连接
test-connection:
	@echo "正在测试数据库连接..."
	@go run $(SRC_DIR)/main.go -c $(CONFIG_FILE)

# 清理构建产物
clean:
	@echo "正在清理构建产物..."
	@rm -f $(BINARY_NAME)
	@rm -f $(ERROR_FILE)
	@rm -f $(CONVERSION_FILE)
	@echo "清理完成！"

# 显示帮助信息
help:
	@echo "MySQL2PG - MySQL到PostgreSQL转换工具"
	@echo "使用方法: make [命令]"
	@echo ""
	@echo "可用命令:"
	@echo "  build            构建项目，生成可执行文件"
	@echo "  run              运行项目，使用默认配置文件"
	@echo "  test-connection  测试数据库连接，使用测试配置文件"
	@echo "  clean            清理构建产物"
	@echo "  help             显示帮助信息"
	@echo ""
	@echo "示例:"
	@echo "  make build       # 构建项目"
	@echo "  make run         # 运行项目"
	@echo "  make clean       # 清理构建产物"