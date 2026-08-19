package postgres

import (
	"strings"
	"testing"
)

// TestEvaluateRowCountValidation 行数校验评估：
// truncate_before_sync=true 且不一致必须返回错误终止迁移（兑现文档承诺），
// truncate=false 时不一致仅记录不阻断
func TestEvaluateRowCountValidation(t *testing.T) {
	tests := []struct {
		name       string
		mysqlCount int64
		pgCount    int64
		truncate   bool
		wantResult string
		wantErr    bool
	}{
		{
			name:       "行数一致",
			mysqlCount: 100,
			pgCount:    100,
			truncate:   true,
			wantResult: "数据一致",
			wantErr:    false,
		},
		{
			name:       "一致时 truncate=false 同样通过",
			mysqlCount: 100,
			pgCount:    100,
			truncate:   false,
			wantResult: "数据一致",
			wantErr:    false,
		},
		{
			name:       "不一致且 truncate=true 必须报错终止",
			mysqlCount: 100,
			pgCount:    80,
			truncate:   true,
			wantResult: "数据不一致",
			wantErr:    true,
		},
		{
			name:       "不一致且 truncate=false 仅记录不阻断",
			mysqlCount: 100,
			pgCount:    150,
			truncate:   false,
			wantResult: "数据不一致",
			wantErr:    false,
		},
		{
			name:       "零行与零行一致",
			mysqlCount: 0,
			pgCount:    0,
			truncate:   true,
			wantResult: "数据一致",
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluateRowCountValidation("t_test", tt.mysqlCount, tt.pgCount, tt.truncate)
			if result != tt.wantResult {
				t.Errorf("result = %q, want %q", result, tt.wantResult)
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				if err == nil || !strings.Contains(err.Error(), "t_test") {
					t.Errorf("错误信息应包含表名，实际: %v", err)
				}
				if !strings.Contains(err.Error(), "truncate_before_sync=true") {
					t.Errorf("错误信息应说明终止原因，实际: %v", err)
				}
			}
		})
	}
}
