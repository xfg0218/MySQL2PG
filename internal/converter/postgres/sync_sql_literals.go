package postgres

import (
	"strconv"
	"strings"
)

// segKind SQL 文本段类型
type segKind int

const (
	segCode   segKind = iota // 普通代码区域
	segString                // 单引号/双引号字符串字面量
	segIdent                 // 反引号标识符
)

// sqlSegment SQL 文本中的一个连续区域
type sqlSegment struct {
	text string
	kind segKind
}

const sqlWhitespace = " \t\r\n\f\v"

// splitSQLSegments 将 SQL 拆分为代码段与字面量段。
// 识别单/双引号字符串（支持 \' \" \\ 反斜杠转义与 ” "" 双写转义）
// 和反引号标识符（支持 “ 双写转义），供引号感知的文本变换使用。
// 未闭合的字面量按到文本末尾处理。
func splitSQLSegments(sql string) []sqlSegment {
	var segs []sqlSegment
	var code strings.Builder
	i, n := 0, len(sql)

	flushCode := func() {
		if code.Len() > 0 {
			segs = append(segs, sqlSegment{text: code.String(), kind: segCode})
			code.Reset()
		}
	}

	for i < n {
		ch := sql[i]
		switch ch {
		case '\'', '"':
			flushCode()
			quote := ch
			j := i + 1
			for j < n {
				c := sql[j]
				if c == '\\' {
					j += 2
					continue
				}
				if c == quote {
					if j+1 < n && sql[j+1] == quote {
						j += 2 // '' 或 "" 双写转义
						continue
					}
					j++
					break
				}
				j++
			}
			if j > n {
				j = n
			}
			segs = append(segs, sqlSegment{text: sql[i:j], kind: segString})
			i = j
		case '`':
			flushCode()
			j := i + 1
			for j < n {
				if sql[j] == '`' {
					if j+1 < n && sql[j+1] == '`' {
						j += 2 // `` 双写转义
						continue
					}
					j++
					break
				}
				j++
			}
			if j > n {
				j = n
			}
			segs = append(segs, sqlSegment{text: sql[i:j], kind: segIdent})
			i = j
		default:
			code.WriteByte(ch)
			i++
		}
	}
	flushCode()
	return segs
}

// compressWhitespaceOutsideLiterals 压缩代码区域的连续空白为单个空格，
// 字符串字面量与反引号标识符内容原样保留。
// 代码段与相邻段之间的单个空格分隔保留，避免产生 SELECT'x' 这类粘连。
func compressWhitespaceOutsideLiterals(sql string) string {
	var b strings.Builder
	for _, seg := range splitSQLSegments(sql) {
		if seg.kind != segCode {
			b.WriteString(seg.text)
			continue
		}
		joined := strings.Join(strings.Fields(seg.text), " ")
		if len(seg.text) > 0 && strings.IndexByte(sqlWhitespace, seg.text[0]) != -1 {
			joined = " " + joined
		}
		if len(seg.text) > 0 && strings.IndexByte(sqlWhitespace, seg.text[len(seg.text)-1]) != -1 {
			joined += " "
		}
		b.WriteString(joined)
	}
	return b.String()
}

// backtickIdentToPG 将反引号标识符段转换为 PostgreSQL 双引号标识符。
// MySQL 的 “ 转义对应标识符内的单个 `，转换后需按 PG 规则双写为 ""。
func backtickIdentToPG(seg string) string {
	if len(seg) < 2 || !strings.HasSuffix(seg, "`") {
		return seg // 未闭合，按原文处理
	}
	inner := seg[1 : len(seg)-1]
	var b strings.Builder
	b.WriteByte('"')
	for i := 0; i < len(inner); i++ {
		if inner[i] == '`' && i+1 < len(inner) && inner[i+1] == '`' {
			b.WriteByte('"')
			i++
		} else if inner[i] == '"' {
			b.WriteString(`""`)
		} else {
			b.WriteByte(inner[i])
		}
	}
	b.WriteByte('"')
	return b.String()
}

// replaceBackticksOutsideLiterals 仅将字符串字面量之外的反引号标识符
// 转换为双引号形式；字面量内部的反引号原样保留。
func replaceBackticksOutsideLiterals(sql string) string {
	var b strings.Builder
	for _, seg := range splitSQLSegments(sql) {
		if seg.kind == segIdent {
			b.WriteString(backtickIdentToPG(seg.text))
		} else {
			b.WriteString(seg.text)
		}
	}
	return b.String()
}

// findKeywordOutsideLiterals 在字符串字面量之外的代码区域查找关键字
// （大小写不敏感），返回绝对位置；未找到返回 -1。
func findKeywordOutsideLiterals(sql, keyword string, from int) int {
	if from < 0 {
		from = 0
	}
	offset := 0
	lowerKW := strings.ToLower(keyword)
	for _, seg := range splitSQLSegments(sql) {
		start := offset
		offset += len(seg.text)
		if seg.kind != segCode || offset <= from {
			continue
		}
		searchFrom := from - start
		if searchFrom < 0 {
			searchFrom = 0
		}
		idx := strings.Index(strings.ToLower(seg.text[searchFrom:]), lowerKW)
		if idx != -1 {
			return start + searchFrom + idx
		}
	}
	return -1
}

// literalPlaceholderPrefix 字面量占位符前缀（全小写、无空白/括号/引号，
// 可安全穿过空白压缩、小写化与函数名替换等变换）
const literalPlaceholderPrefix = "__m2pg_lit_"

// literalMask 字面量遮蔽器：将字符串字面量替换为占位符，
// 使全局正则/字符串变换不会破坏字面量内容，变换完成后恢复原文。
// 注意：遮蔽只处理字符串字面量；若文本中仍有反引号标识符，
// 应先调用 replaceBackticksOutsideLiterals。
type literalMask struct {
	literals []string // 按遮蔽顺序保存的原始字面量（含引号）
}

// newLiteralMask 创建字面量遮蔽器
func newLiteralMask() *literalMask {
	return &literalMask{}
}

// mask 将字符串字面量替换为占位符，返回遮蔽后的文本
func (m *literalMask) mask(sql string) string {
	var b strings.Builder
	for _, seg := range splitSQLSegments(sql) {
		if seg.kind == segString {
			b.WriteString(literalPlaceholderPrefix)
			b.WriteString(strconv.Itoa(len(m.literals)))
			b.WriteString("__")
			m.literals = append(m.literals, seg.text)
		} else {
			b.WriteString(seg.text)
		}
	}
	return b.String()
}

// unmask 将占位符恢复为原始字面量
func (m *literalMask) unmask(sql string) string {
	for i, lit := range m.literals {
		sql = strings.Replace(sql, literalPlaceholderPrefix+strconv.Itoa(i)+"__", lit, 1)
	}
	return sql
}
