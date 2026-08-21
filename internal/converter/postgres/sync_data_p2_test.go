package postgres

import (
	"testing"
)

// TestEstimateRowWidth P2-07：按列类型估算行宽
func TestEstimateRowWidth(t *testing.T) {
	tests := []struct {
		name    string
		types   map[string]string
		wantMin int
		wantMax int
	}{
		{"纯整数列", map[string]string{"a": "bigint(20)", "b": "int"}, 16, 16},
		{"datetime 不误判为 date", map[string]string{"a": "datetime(6)"}, 12, 12},
		{"date 单独计宽", map[string]string{"a": "date"}, 4, 4},
		{"varchar 按声明长度", map[string]string{"a": "varchar(100)"}, 100, 100},
		{"varchar 超长封顶 512", map[string]string{"a": "varchar(4000)"}, 512, 512},
		{"大对象统一 1024", map[string]string{"a": "longtext", "b": "json"}, 2048, 2048},
		{"decimal 计 16", map[string]string{"a": "decimal(10,2)"}, 16, 16},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := estimateRowWidth(tt.types)
			if got < tt.wantMin || got > tt.wantMax {
				t.Errorf("estimateRowWidth(%v) = %d, want [%d, %d]", tt.types, got, tt.wantMin, tt.wantMax)
			}
		})
	}
}

// TestExtractTypeLength 类型长度提取
func TestExtractTypeLength(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"varchar(255)", 255},
		{"decimal(10,2)", 10},
		{"text", 0},
		{"varchar(abc)", 0},
	}
	for _, tt := range tests {
		if got := extractTypeLength(tt.input); got != tt.want {
			t.Errorf("extractTypeLength(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

// TestAdaptiveBatchSizes P2-07：内存预算封顶逻辑
func TestAdaptiveBatchSizes(t *testing.T) {
	// 窄表（16 字节/行）：50000 行仅 800KB，远低于预算，保持原值
	readBatch, insertBatch := adaptiveBatchSizes(50000, 50000, 16)
	if readBatch != 50000 || insertBatch != 50000 {
		t.Errorf("窄表不应下调批大小：got (%d, %d)", readBatch, insertBatch)
	}

	// 超宽表（1MB/行）：预算 64MB → 最多 64 行
	readBatch, insertBatch = adaptiveBatchSizes(50000, 50000, 1024*1024)
	wantRows := int64(maxBatchMemoryBytes) / (1024 * 1024)
	if readBatch != wantRows || int64(insertBatch) != wantRows {
		t.Errorf("超宽表应下调至 %d 行：got (%d, %d)", wantRows, readBatch, insertBatch)
	}

	// 极端行宽：至少保留 1 行
	readBatch, insertBatch = adaptiveBatchSizes(50000, 50000, maxBatchMemoryBytes*2)
	if readBatch != 1 || insertBatch != 1 {
		t.Errorf("极端行宽应保底 1 行：got (%d, %d)", readBatch, insertBatch)
	}

	// 行宽为 0（无列类型信息）：原样返回
	readBatch, insertBatch = adaptiveBatchSizes(50000, 50000, 0)
	if readBatch != 50000 || insertBatch != 50000 {
		t.Errorf("零行宽应原样返回：got (%d, %d)", readBatch, insertBatch)
	}
}
