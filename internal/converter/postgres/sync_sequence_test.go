package postgres

import "testing"

// TestParseAutoIncrementInfo 验证从 MySQL DDL 解析自增列与表级起始值
func TestParseAutoIncrementInfo(t *testing.T) {
	tests := []struct {
		name      string
		ddl       string
		wantCol   string
		wantStart int64
		wantOK    bool
	}{
		{
			name: "标准 SHOW CREATE TABLE 输出",
			ddl: "CREATE TABLE `users` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  `name` varchar(50) DEFAULT NULL,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB AUTO_INCREMENT=101 DEFAULT CHARSET=utf8mb4;",
			wantCol:   "id",
			wantStart: 101,
			wantOK:    true,
		},
		{
			name: "无表级 AUTO_INCREMENT 选项",
			ddl: "CREATE TABLE `t` (\n" +
				"  `id` int NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB;",
			wantCol:   "id",
			wantStart: 0,
			wantOK:    true,
		},
		{
			name: "无自增列",
			ddl: "CREATE TABLE `t` (\n" +
				"  `id` int NOT NULL,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB AUTO_INCREMENT=5;",
			wantCol:   "",
			wantStart: 5,
			wantOK:    false,
		},
		{
			name: "空表带起始值",
			ddl: "CREATE TABLE `orders` (\n" +
				"  `order_id` bigint NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`order_id`)\n" +
				") ENGINE=InnoDB AUTO_INCREMENT=5000;",
			wantCol:   "order_id",
			wantStart: 5000,
			wantOK:    true,
		},
		{
			name: "列名不带反引号",
			ddl: "CREATE TABLE t (\n" +
				"  id int AUTO_INCREMENT PRIMARY KEY,\n" +
				"  name varchar(10)\n" +
				")",
			wantCol:   "id",
			wantStart: 0,
			wantOK:    true,
		},
		{
			name: "小写关键字与混合大小写列名",
			ddl: "CREATE TABLE `t` (\n" +
				"  `UserId` int(11) not null auto_increment,\n" +
				"  primary key (`UserId`)\n" +
				") engine=innodb auto_increment=42;",
			wantCol:   "UserId",
			wantStart: 42,
			wantOK:    true,
		},
		{
			name: "ZEROFILL 无符号自增",
			ddl: "CREATE TABLE `t` (\n" +
				"  `seq` bigint unsigned zerofill NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`seq`)\n" +
				") ENGINE=InnoDB AUTO_INCREMENT=99;",
			wantCol:   "seq",
			wantStart: 99,
			wantOK:    true,
		},
		{
			name:      "无自增列且无选项",
			ddl:       "CREATE TABLE `t` (\n  `id` int\n)",
			wantCol:   "",
			wantStart: 0,
			wantOK:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col, start, ok := ParseAutoIncrementInfo(tt.ddl)
			if ok != tt.wantOK {
				t.Errorf("ParseAutoIncrementInfo() ok = %v, want %v", ok, tt.wantOK)
			}
			if col != tt.wantCol {
				t.Errorf("ParseAutoIncrementInfo() col = %q, want %q", col, tt.wantCol)
			}
			if start != tt.wantStart {
				t.Errorf("ParseAutoIncrementInfo() start = %d, want %d", start, tt.wantStart)
			}
		})
	}
}

// TestParseAutoIncrementInfo_TableOptionsNotMisidentified 表级选项行不应被误判为自增列
func TestParseAutoIncrementInfo_TableOptionsNotMisidentified(t *testing.T) {
	ddl := "CREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `name` varchar(20)\n" +
		") ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4;"

	_, start, ok := ParseAutoIncrementInfo(ddl)
	if ok {
		t.Error("无自增列时 ok 应为 false（表级选项行不应被误判为列定义）")
	}
	if start != 7 {
		t.Errorf("start = %d, want 7", start)
	}
}
