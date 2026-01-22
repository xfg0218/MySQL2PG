#!/bin/bash
# MySQL8.0
# 111.229.183.135
# MySQL 7.5
# 43.143.103.17

# 测试所有数据库
DATABASES=("test_db")

# 日志文件
LOG_FILE="auto_migrate.log"
ERROR_LOG="auto_migrate_errors.log"

# 配置文件路径
CONFIG_FILE="../config.yml"

# 可执行文件路径
EXECUTABLE="../mysql2pg"

# 初始化日志文件
echo "开始数据库迁移测试：$(date)" > $LOG_FILE
echo "开始数据库迁移测试：$(date)" > $ERROR_LOG

# 遍历所有数据库
for DB in "${DATABASES[@]}"; do
    echo -e "\n=== 开始处理数据库：$DB ==="
    echo -e "\n=== 开始处理数据库：$DB ===" >> $LOG_FILE
    echo -e "\n=== 开始处理数据库：$DB ===" >> $ERROR_LOG
    
    # 修改配置文件
    echo "修改配置文件，设置数据库为 $DB..."
    sed -i '' "s/^  database: .*/  database: $DB/" $CONFIG_FILE
    
    # 检查修改是否成功
    if [ $? -ne 0 ]; then
        echo "错误：无法修改配置文件！" >> $ERROR_LOG
        continue
    fi
    
    echo "配置文件修改成功，当前数据库设置为："
    grep "database:" $CONFIG_FILE
    
    # 运行迁移工具
    echo "运行迁移工具..."
    $EXECUTABLE -c $CONFIG_FILE >> $LOG_FILE 2>> $ERROR_LOG
    
    # 检查迁移结果
    if [ $? -eq 0 ]; then
        echo "数据库 $DB 迁移成功！" >> $LOG_FILE
    else
        echo "数据库 $DB 迁移失败，开始尝试自动修复..." >> $ERROR_LOG
        
        # 这里可以添加自动修复逻辑
        # 例如：检查错误日志，识别常见问题并修复
        
        # 检查是否有用户相关错误（之前遇到的问题）
        if grep -q "The user specified as a definer" $ERROR_LOG; then
            echo "发现用户定义者错误，重新运行迁移..." >> $ERROR_LOG
            $EXECUTABLE -c $CONFIG_FILE >> $LOG_FILE 2>> $ERROR_LOG
            
            if [ $? -eq 0 ]; then
                echo "修复成功，数据库 $DB 迁移完成！" >> $LOG_FILE
            else
                echo "修复失败，数据库 $DB 迁移仍未成功！" >> $ERROR_LOG
            fi
        fi
    fi

    echo "=== 数据库 $DB 处理完成 ==="
done

echo -e "\n所有数据库迁移测试完成：$(date)" >> $LOG_FILE
echo -e "\n所有数据库迁移测试完成：$(date)" >> $ERROR_LOG
echo -e "\n所有数据库迁移测试完成！"
echo "详细日志请查看：$LOG_FILE"
echo "错误日志请查看：$ERROR_LOG"
