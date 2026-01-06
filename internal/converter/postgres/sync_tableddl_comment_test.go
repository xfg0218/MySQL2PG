package postgres

import (
	"strings"
	"testing"
)

func TestConvertTableDDL_CommentColumn(t *testing.T) {
	mysqlDDL := `CREATE TABLE "slb_member" ( 
   "pool_id" BIGINT comment '所属池id',
   "net_card_id" BIGINT comment '网卡id',
   "v4_address" VARCHAR(255)
)`

	pgDDL, err := ConvertTableDDL(mysqlDDL, true)
	if err != nil {
		t.Fatalf("转换DDL失败: %v", err)
	}

	t.Log("转换后的DDL:")
	t.Log(pgDDL)

	if strings.Contains(pgDDL, "comment '") {
		t.Error("生成的DDL中不应该包含小写comment关键字")
	}

	if !strings.Contains(pgDDL, "COMMENT ON COLUMN") {
		t.Error("生成的DDL中应该包含COMMENT ON COLUMN语句")
	}

	if !strings.Contains(pgDDL, `"pool_id"`) {
		t.Error("生成的DDL中缺少 pool_id 字段")
	}

	if !strings.Contains(pgDDL, `"net_card_id"`) {
		t.Error("生成的DDL中缺少 net_card_id 字段")
	}

	if !strings.Contains(pgDDL, `"v4_address"`) {
		t.Error("生成的DDL中缺少 v4_address 字段")
	}

	if !strings.Contains(pgDDL, `COMMENT ON COLUMN "slb_member"."pool_id" IS '所属池id'`) {
		t.Error("生成的DDL中缺少 pool_id 的COMMENT语句")
	}

	if !strings.Contains(pgDDL, `COMMENT ON COLUMN "slb_member"."net_card_id" IS '网卡id'`) {
		t.Error("生成的DDL中缺少 net_card_id 的COMMENT语句")
	}
}

func TestConvertTableDDL_CommentColumnWithDoubleQuotes(t *testing.T) {
	mysqlDDL := `CREATE TABLE "slb_listener" ( 
   "deleted" BIGINT not null default '0',
   "loadbalancer_id" BIGINT comment "所属负载均衡器id"
)`

	pgDDL, err := ConvertTableDDL(mysqlDDL, true)
	if err != nil {
		t.Fatalf("转换DDL失败: %v", err)
	}

	t.Log("转换后的DDL (双引号注释):")
	t.Log(pgDDL)

	if strings.Contains(pgDDL, "comment ") && !strings.Contains(pgDDL, "COMMENT ON COLUMN") {
		t.Error("生成的DDL中不应该包含小写comment关键字")
	}

	if !strings.Contains(pgDDL, `COMMENT ON COLUMN "slb_listener"."loadbalancer_id" IS '所属负载均衡器id'`) {
		t.Error("生成的DDL中缺少 loadbalancer_id 的COMMENT语句")
	}
}

func TestConvertTableDDL_CommentColumnFull(t *testing.T) {
	mysqlDDL := `CREATE TABLE "slb_member" ( 
   "pool_id" BIGINT comment '所属池id',
   "net_card_id" BIGINT comment '网卡id',
   "v4_address" VARCHAR(255),
   "v6_address" VARCHAR(255),
   "protocol_port" BIGINT,
   "weight" BIGINT,
   "member_id" VARCHAR(255),
   "member_uuid" VARCHAR(255),
   "deleted" BIGINT not null default '0',
   PRIMARY KEY ("id")
)`

	pgDDL, err := ConvertTableDDL(mysqlDDL, true)
	if err != nil {
		t.Fatalf("转换DDL失败: %v", err)
	}

	t.Log("完整转换后的DDL:")
	t.Log(pgDDL)

	fieldCount := strings.Count(pgDDL, `"`)
	t.Logf("引号数量: %d, 字段数: %d", fieldCount, fieldCount/2)

	if fieldCount < 18 {
		t.Errorf("字段数量不足，期望至少9个字段(18个引号)，实际有 %d 个引号", fieldCount)
	}

	if !strings.Contains(pgDDL, `COMMENT ON COLUMN "slb_member"."pool_id" IS '所属池id'`) {
		t.Error("生成的DDL中缺少 pool_id 的COMMENT语句")
	}

	if !strings.Contains(pgDDL, `COMMENT ON COLUMN "slb_member"."net_card_id" IS '网卡id'`) {
		t.Error("生成的DDL中缺少 net_card_id 的COMMENT语句")
	}

	commentCount := strings.Count(pgDDL, "COMMENT ON COLUMN")
	if commentCount != 2 {
		t.Errorf("期望2个COMMENT ON COLUMN语句，实际有 %d 个", commentCount)
	}
}
