## dbr定制化

## github.com/gocraft/dbr

### 1. 添加日志输出

**github.com/gocraft/dbr/event.go** 

```go
//添加
var (
	showSQLLevel int
	logPrintFunc func(args ...interface{})
)

//是否打印SQL
// level 0：不打印SQL；1：只打印err；2：打印全部
func ShowSQL(level int, logPrint ...func(args ...interface{})) {
	showSQLLevel = level
	if len(logPrint) > 0 {
		logPrintFunc = logPrint[0]
	}
}

//改造日志打印事件
func (n *NullEventReceiver) EventErrKv(eventName string, err error, kvs map[string]string) error {
	if showSQLLevel >= 1 {
		var sql, useStr string
		if s, ok := kvs["sql"]; ok {
			sql = s
		}
		use := time.Millisecond
		if s, ok := kvs["time"]; ok {
			useStr = s
			useInt, _ := strconv.Atoi(useStr)
			use = use * time.Duration(useInt)
		} else {
			useStr = "-"
			use = use * 0
		}
		sqlLog := fmt.Sprintf("[ERR %sms] [%v] %s", useStr, err, sql)
		if logPrintFunc != nil {
			logPrintFunc(sqlLog)
		} else {
			fmt.Println(fmt.Sprintf("[DBR]%s %s", time.Now().Add(-use).Format("2006/01/02 15:04:05.000"), sqlLog))
		}
	}
	return err
}

func (n *NullEventReceiver) TimingKv(eventName string, nanoseconds int64, kvs map[string]string) {
	if showSQLLevel >= 2 {
		var sql string
		if s, ok := kvs["sql"]; ok {
			sql = s
		}
		sqlLog := fmt.Sprintf("[OK %dms] %s", nanoseconds/1e6, sql)
		if logPrintFunc != nil {
			logPrintFunc(sqlLog)
		} else {
			fmt.Println(fmt.Sprintf("[DBR]%s %s", time.Now().Add(-time.Duration(nanoseconds)).Format("2006/01/02 15:04:05.000"), sqlLog))
		}
	}
}
```

**github.com/gocraft/dbr/dbr.go** 

```go
//修改
func exec(ctx context.Context, runner runner, log EventReceiver, builder Builder, d Dialect) (sql.Result, error) {
    ...
	startTime := time.Now()
	//defer func() {
	//	log.TimingKv("dbr.exec", time.Since(startTime).Nanoseconds(), kvs{
	//		"sql": query,
	//	})
	//}()

	traceImpl, hasTracingImpl := log.(TracingEventReceiver)
	if hasTracingImpl {
		ctx = traceImpl.SpanStart(ctx, "dbr.exec", query)
		defer traceImpl.SpanFinish(ctx)
	}

	result, err := runner.ExecContext(ctx, query, value...)
	if err != nil {
		if hasTracingImpl {
			traceImpl.SpanError(ctx, err)
		}
		return result, log.EventErrKv("dbr.exec.exec", err, kvs{
			"sql":  query,
			"time": strconv.FormatInt(time.Since(startTime).Nanoseconds()/1e6, 10),
		})
	}

	log.TimingKv("dbr.exec", time.Since(startTime).Nanoseconds(), kvs{
		"sql": query,
	})
	return result, nil
}

//修改
func queryRows(ctx context.Context, runner runner, log EventReceiver, builder Builder, d Dialect) (string, *sql.Rows, error) {
    ...
    startTime := time.Now()
	//defer func() {
	//	log.TimingKv("dbr.select", time.Since(startTime).Nanoseconds(), kvs{
	//		"sql": query,
	//	})
	//}()

	traceImpl, hasTracingImpl := log.(TracingEventReceiver)
	if hasTracingImpl {
		ctx = traceImpl.SpanStart(ctx, "dbr.select", query)
		defer traceImpl.SpanFinish(ctx)
	}

	rows, err := runner.QueryContext(ctx, query, value...)
	if err != nil {
		if hasTracingImpl {
			traceImpl.SpanError(ctx, err)
		}
		return query, nil, log.EventErrKv("dbr.select.load.query", err, kvs{
			"sql":  query,
			"time": strconv.FormatInt(time.Since(startTime).Nanoseconds()/1e6, 10),
		})
	}

	return query, rows, nil
}

//修改
func query(ctx context.Context, runner runner, log EventReceiver, builder Builder, d Dialect, dest interface{}) (int, error) {
    ...
    startTime := time.Now()
	query, rows, err := queryRows(ctx, runner, log, builder, d)
	if err != nil {
		return 0, err
	}
	count, err := Load(rows, dest)
	if err != nil {
		return 0, log.EventErrKv("dbr.select.load.scan", err, kvs{
			"sql":  query,
			"time": strconv.FormatInt(time.Since(startTime).Nanoseconds()/1e6, 10),
		})
	}

	log.TimingKv("dbr.select", time.Since(startTime).Nanoseconds(), kvs{
		"sql": query,
	})
	return count, nil
}
```



### 2. 添加获取组装好的SQL的方法

**github.com/gocraft/dbr/dbr.go** 

```go
//添加
//获取SQL
func getSQL(builder Builder, d Dialect) (string, error) {
	i := interpolator{
		Buffer:       NewBuffer(),
		Dialect:      d,
		IgnoreBinary: true,
	}
	err := i.encodePlaceholder(builder, true)
	return i.String(), err
}
```

**github.com/gocraft/dbr/select.go** 等select、insert、update、delete

```go
//添加
//获取SQL
func (b *DeleteStmt) GetSQL() (string, error) {
	b1 := *b
	b2 := &b1
	return getSQL(b2, b2.Dialect)
}
```



### 3. 新增insert支持插入map

**github.com/gocraft/dbr/insert.go**

```go
//新增 开始
//插入map，key为column，value为value
func (b *InsertStmt) Map(kv map[string]interface{}) *InsertStmt {
  value := []interface{}{}
  if len(b.Column) == 0 {
    for k, v := range kv {
      b.Column = append(b.Column, k)
      value = append(value, v)
    }
  } else {
    for _, col := range b.Column {
      v := kv[col]
      value = append(value, v)
    }
  }
  b.Value = append(b.Value, value)
  return b
}
//新增 结束
```



### 4. 修改update支持set的value值为dbr.Expr表达式

**github.com/gocraft/dbr/update.go**

```go
func (b *UpdateStmt) Build(d Dialect, buf Buffer) error {
...
   for col, v := range b.Value {
      if i > 0 {
         buf.WriteString(", ")
      }
      buf.WriteString(d.QuoteIdent(col))
      buf.WriteString(" = ")
      buf.WriteString(placeholder)

      buf.WriteValue(v)
      i++
   }
...
}
```

改为：

```go
func (b *UpdateStmt) Build(d Dialect, buf Buffer) error {
...
	for col, v := range b.Value {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(d.QuoteIdent(col))
		buf.WriteString(" = ")
        //修改 开始
		switch v := v.(type) {
		case raw:
			v.Build(d,buf)
		default:
			buf.WriteString(placeholder)
			buf.WriteValue(v)
		}
        //修改 结束
		i++
	}
...
}
```



### 5. select的from方法.增加了第二个参数.可以用于增加别名

**github.com/gocraft/dbr/select.go** 

```go
type SelectStmt struct {
    //添加
    TableAs   string
}

func (b *SelectStmt) Build(d Dialect, buf Buffer) error {
    //改为
    if b.Table != nil {
		buf.WriteString(" FROM ")
		switch table := b.Table.(type) {
		case Builder:
			buf.WriteString("(")
			table.Build(d,buf)
			buf.WriteString(")")
		case string:
			// FIXME: no quote ident
			buf.WriteString(table)
		default:
			buf.WriteString(placeholder)
			buf.WriteValue(table)
		}
		if b.TableAs!=""{
			buf.WriteString(" As ")
			buf.WriteString(b.TableAs)
		}
		if len(b.JoinTable) > 0 {
			for _, join := range b.JoinTable {
				err := join.Build(d, buf)
				if err != nil {
					return err
				}
			}
		}
	}
}

//改为
func (b *SelectStmt) From(table interface{},as ...string) *SelectStmt {
	b.Table = table
	if len(as)>0{
		b.TableAs=as[0]
	}
	return b
}
```

**github.com/xs-soft/dbr/union.go** 

```go
func (u *union) Build(d Dialect, buf Buffer) error {
	for i, b := range u.builder {
		if i > 0 {
			buf.WriteString(" UNION ")
			if u.all {
				buf.WriteString("ALL ")
			}
		}
		//改为
		b.Build(d,buf)
		//buf.WriteString(placeholder)
		//buf.WriteValue(b)
	}
	return nil
}
```



### 6. 修改join不支持别名问题

**github.com/gocraft/dbr/join.go**

```go
func join(t joinType, table interface{}, on interface{}) Builder {
		...
		case string:
			buf.WriteString(d.QuoteIdent(table))
		...
}
```

改为：

```go
func join(t joinType, table interface{}, on interface{}) Builder {
		...
		case string:
    		//修改 开始
			buf.WriteString(table)
    		//修改 结束
		...
}
```



### 7. 修改where条件dbr.And支持输入nil，自动筛除

**github.com/gocraft/dbr/condition.go**

```go 
func buildCond(d Dialect, buf Buffer, pred string, cond ...Builder) error {
   for i, c := range cond {
      if i > 0 {
         buf.WriteString(" ")
         buf.WriteString(pred)
         buf.WriteString(" ")
      }
      buf.WriteString("(")
      err := c.Build(d, buf)
      if err != nil {
         return err
      }
      buf.WriteString(")")
   }
   return nil
}
```

改为：

```go
func buildCond(d Dialect, buf Buffer, pred string, cond ...Builder) error {
    //修改 开始
   i := 0
   for _, c := range cond {
      if c == nil {
         continue
      }
      if i > 0 {
         buf.WriteString(" ")
         buf.WriteString(pred)
         buf.WriteString(" ")
      }
      buf.WriteString("(")
      err := c.Build(d, buf)
      if err != nil {
         return err
      }
      buf.WriteString(")")
      i++
   }
    //修改 结束
   return nil
}
```



### 8. 添加select锁

**github.com/gocraft/dbr/select.go**

```go
type SelectStmt struct {
    ...
	//新增 开始
	IsLock    *bool
    //新增 结束
    ...
}
```

```go
func (b *SelectStmt) Build(d Dialect, buf Buffer) error {
    ...
	if b.OffsetCount >= 0 {
		buf.WriteString(" OFFSET ")
		buf.WriteString(strconv.FormatInt(b.OffsetCount, 10))
	}
    //新增 开始
    //如果未设置Lock.并且是实物
	if b.IsLock == nil {
		if _,ok := b.runner.(*Tx); ok {
			buf.WriteString(" FOR UPDATE ")
		}
	}else if *b.IsLock {
		buf.WriteString(" FOR UPDATE ")
	}
    //新增 结束
	return nil
}
```

```go
//新增 开始
func (b *SelectStmt) Lock(l bool) *SelectStmt {
   b.IsLock = &l
   return b
}
//新增 结束
```



### 9.  where条件添加Between方法

**github.com/gocraft/dbr/condition.go**

```go
// Between is `BETWEEN ? AND ?`.
 func Between(column string, minVal interface{}, maxVal interface{}) Builder {
 	return BuildFunc(func(d Dialect, buf Buffer) error {
 		return buildBetween(d, buf, column, minVal, maxVal)
 	})
 }

  func buildBetween(d Dialect, buf Buffer, column string, minVal interface{}, maxVal interface{}) error {
 	buf.WriteString(d.QuoteIdent(column))
 	buf.WriteString(" BETWEEN ")
 	buf.WriteString(placeholder)
 	buf.WriteString(" AND ")
 	buf.WriteString(placeholder)

  	buf.WriteValue(minVal)
 	buf.WriteValue(maxVal)
 	return nil
 }
```



### 10. select添加获取总条数的Count方法

**github.com/gocraft/dbr/dbr.go**

```go
// 添加
func count(ctx context.Context, runner runner, log EventReceiver, builder Builder, d Dialect) (int, error) {
 	i := interpolator{
 		Buffer:       NewBuffer(),
 		Dialect:      d,
 		IgnoreBinary: true,
 	}
 	err := i.encodePlaceholder(builder, true)
 	query, value := i.String(), i.Value()
 	query = fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS count", query)
 	if err != nil {
 		return 0, log.EventErrKv("dbr.select.interpolate", err, kvs{
 			"sql":  query,
 			"args": fmt.Sprint(value),
 		})
 	}

  	startTime := time.Now()

  	traceImpl, hasTracingImpl := log.(TracingEventReceiver)
 	if hasTracingImpl {
 		ctx = traceImpl.SpanStart(ctx, "dbr.select", query)
 		defer traceImpl.SpanFinish(ctx)
 	}

  	rows, err := runner.QueryContext(ctx, query, value...)
 	if err != nil {
 		if hasTracingImpl {
 			traceImpl.SpanError(ctx, err)
 		}
 		return 0, log.EventErrKv("dbr.select.load.query", err, kvs{
 			"sql":  query,
 			"time": strconv.FormatInt(time.Since(startTime).Nanoseconds()/1e6, 10),
 		})
 	}
 	defer rows.Close()
 	var count int
 	if rows.Next() {
 		rows.Scan(&count)
 	}

  	log.TimingKv("dbr.select", time.Since(startTime).Nanoseconds(), kvs{
 		"sql": query,
 	})
 	return count, nil
 }
```

**github.com/gocraft/dbr/select.go**

```go
// 添加
//获取总条数
 func (b *SelectStmt) Count() (int, error) {
 	b1 := *b
 	b2 := &b1
 	return count(context.Background(), b2.runner, b2.EventReceiver, b2, b2.Dialect)
 }
```





