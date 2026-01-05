package postgres

import (
	"fmt"
	"strings"
	"testing"
)

func TestConvertTableDDL_OrganizationCode(t *testing.T) {
	mysqlDDL := `create table ucs_customer (
  customer_id bigint(20) not null default 0 comment '用户记录唯一标识',
  login_name varchar(128) default null comment '用户登录名称,不可修改',
  customer_password varchar(64) default null,
  account_id bigint(20) default null comment '账户记录唯一标识',
  customer_type int(4) default null comment '用户类型,0为个人用户,1为企业用户',
  customer_name varchar(128) default null comment '用户名称,可修改',
  customer_status int(3) default null comment '用户记录状态,0为未激活状态,1为正常状态,2为失效状态3为冻结状态',
  customer_email varchar(128) default null comment '用户邮箱',
  customer_mobile varchar(32) default null comment '用户移动电话',
  customer_phone varchar(32) default null comment '用户电话,一般为固话',
  customer_fax varchar(32) default null comment '用户传真',
  cert_type int(2) default null comment '证件类型 0 18位身份证；1 16位身份证；2 营业执照;3 军人证；4 护照；',
  cert_num varchar(50) default null comment '证件号码型',
  cert_addr varchar(128) default null comment '证件地址',
  create_date datetime default null comment '用户记录创建时间',
  status_chg_date datetime default null comment '用户记录状态变更时间',
  modify_date datetime default null comment '用户信息修改时间',
  region_id varchar(4) default null comment '用户地域id',
  role_id int(6) default null comment '用户角色id,默认为普通角色',
  compid varchar(1024) default null comment '企业id',
  compname varchar(128) default null comment '企业名称',
  comporgcode varchar(32) default null comment '企业编码',
  compaddress varchar(128) default null comment '企业地址',
  compphone varchar(32) default null comment '企业电话',
  compfax varchar(32) default null comment '企业传真',
  compemail varchar(128) default null comment '企业邮箱',
  dept_id varchar(32) default null comment '部门id',
  position varchar(32) default null comment '职位',
  postcode varchar(32) default null comment '邮编',
  commentinfo varchar(128) default null comment '记录备注说明',
  online int(2) default 0 comment '默认为0，0或者空，为在线用户，不能订购后付费产品，1为线下用户，可订购后付费产品',
  is_admincust int(2) default null comment '1 管理员客户 0 普通客户',
  businesslinkman varchar(128) default null comment '业务联系人',
  organizationcode varchar(128) default null comment '组织机构代码',
  bankcode varchar(32) default null comment '行 号',
  bank varchar(128) default null comment '开户行',
  bankaccount varchar(64) default null comment '银行账号',
  accountopener varchar(128) default '' comment '开户单位',
  record varchar(128) default null comment '网站备案号',
  industry varchar(128) default null comment '所属行业',
  license varchar(128) default null comment '营业执照',
  resourcepool varchar(512) default null,
  tax varchar(128) default null,
  fax varchar(128) default null,
  technicalcontact varchar(128) default null comment '技术联系人',
  financialcontact varchar(128) default null comment '财务联系人',
  emergencycontact varchar(128) default null,
  remarks varchar(512) default null comment '客户备注',
  contract int(16) default null,
  tenant_id varchar(100) default null comment '底层资源池的租户id  ',
  clean_flag varchar(10) default null,
  portal_type int(2) default 1,
  province_code varchar(6) default null,
  primary key (customer_id),
  key c_account_id_squence (account_id) using btree,
  key idx1_ucs_customer (login_name) using btree,
  key customer_id (customer_id)) engine=innodb default charset=utf8mb3 collate=utf8mb3_bin row_format=compressed;`

	pgDDL, err := ConvertTableDDL(mysqlDDL, true)
	if err != nil {
		t.Fatalf("转换DDL失败: %v", err)
	}

	fmt.Println("生成的PostgreSQL DDL:")
	fmt.Println(pgDDL)

	// 检查 organizationcode 是否存在
	if !containsColumn(pgDDL, "organizationcode") {
		t.Error("生成的DDL中缺少 organizationcode 字段")
	}

	// 检查是否有53个字段（52个普通字段 + 1个主键列）
	fieldCount := countFields(pgDDL)
	fmt.Printf("生成的字段数量: %d\n", fieldCount)
	if fieldCount != 53 {
		t.Errorf("期望53个字段，实际生成 %d 个字段", fieldCount)
	}
}

func containsColumn(ddl, columnName string) bool {
	return containsString(ddl, `"`+columnName+`"`) || containsString(ddl, columnName)
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && findSubstring(s, substr) >= 0
}

func findSubstring(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func countFields(ddl string) int {
	count := 0
	for _, line := range splitLines(ddl) {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, `"`) && strings.Contains(trimmed, `"`) && !strings.Contains(trimmed, "PRIMARY KEY") {
			count++
		}
	}
	return count
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}
