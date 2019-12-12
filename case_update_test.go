package dbr

import (
	"fmt"
	"testing"
)

func TestCaseUpdateStmt(t *testing.T) {
	err := CaseUpdate("table").
		// 主键字段 更新字段...
		Columns("id", "a", "b").
		// 主键值	对应更新字段值...
		Values(1, "a1", "b1").
		Values(2, "a2", "b2").
		Values(3, "a3", "b3").
		Values(4, "a4", "b4").
		// 分批每次执行条数
		SetRunLen(3).
		Exec()
	fmt.Println(err)
}
