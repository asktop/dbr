// Package dbr provides additions to Go's database/sql for super fast performance and convenience.
package dbr

import (
	"bytes"
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/gob"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/asktop/dbr/dialect"
)

// Open creates a Connection.
// log can be nil to ignore logging.
func Open(driver, dsn string, log EventReceiver) (*Connection, error) {
	if log == nil {
		log = nullReceiver
	}
	conn, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	var d Dialect
	switch driver {
	case "mysql":
		d = dialect.MySQL
	case "postgres":
		d = dialect.PostgreSQL
	case "sqlite3":
		d = dialect.SQLite3
	default:
		return nil, ErrNotSupported
	}
	return &Connection{DB: conn, EventReceiver: log, Dialect: d}, nil
}

const (
	placeholder = "?"
)

// Connection wraps sql.DB with an EventReceiver
// to send events, errors, and timings.
type Connection struct {
	*sql.DB
	Dialect
	EventReceiver
}

// Session represents a business unit of execution.
//
// All queries in asktop/dbr are made in the context of a session.
// This is because when instrumenting your app, it's important
// to understand which business action the query took place in.
//
// A custom EventReceiver can be set.
//
// Timeout specifies max duration for an operation like Select.
type Session struct {
	*Connection
	EventReceiver
	Timeout time.Duration
}

// GetTimeout returns current timeout enforced in session.
func (sess *Session) GetTimeout() time.Duration {
	return sess.Timeout
}

// NewSession instantiates a Session from Connection.
// If log is nil, Connection EventReceiver is used.
func (conn *Connection) NewSession(log EventReceiver) *Session {
	if log == nil {
		log = conn.EventReceiver // Use parent instrumentation
	}
	return &Session{Connection: conn, EventReceiver: log}
}

// Ensure that tx and session are session runner
var (
	_ SessionRunner = (*Tx)(nil)
	_ SessionRunner = (*Session)(nil)
)

// SessionRunner can do anything that a Session can except start a transaction.
// Both Session and Tx implements this interface.
type SessionRunner interface {
	Select(column ...string) *SelectBuilder
	SelectBySql(query string, value ...interface{}) *SelectBuilder

	InsertInto(table string) *InsertBuilder
	InsertBySql(query string, value ...interface{}) *InsertBuilder

	Update(table string) *UpdateBuilder
	UpdateBySql(query string, value ...interface{}) *UpdateBuilder

	DeleteFrom(table string) *DeleteBuilder
	DeleteBySql(query string, value ...interface{}) *DeleteBuilder
}

type runner interface {
	GetTimeout() time.Duration
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

func exec(ctx context.Context, runner runner, log EventReceiver, builder Builder, d Dialect, customs ...Custom) (sql.Result, error) {
	timeout := runner.GetTimeout()
	if timeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	i := interpolator{
		Buffer:       NewBuffer(),
		Dialect:      d,
		IgnoreBinary: true,
	}
	err := i.encodePlaceholder(builder, true)
	query, value := i.String(), i.Value()
	if err != nil {
		return nil, log.EventErrKv("dbr.exec.interpolate", err, kvs{
			"sql":  query,
			"args": fmt.Sprint(value),
		})
	}

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

	var custom Custom //自定义参数
	if len(customs) > 0 {
		custom = customs[0]
	}

	//redis缓存数据库数据删除
	if custom.isCache {
		//redis缓存数据Key
		dbcacheDataKey := custom.cacheKey
		if dbcacheDataKey != "" {
			err = custom.cache.Del(dbcacheDataKey)
			if err != nil {
				_ = log.EventErrKv("dbr.exec.cache.del", err, kvs{
					"sql":  query,
					"time": strconv.FormatInt(time.Since(startTime).Nanoseconds()/1e6, 10),
				})
			}
		}
	}

	log.TimingKv("dbr.exec", time.Since(startTime).Nanoseconds(), kvs{
		"sql": query,
	})
	return result, nil
}

func queryRows(ctx context.Context, runner runner, log EventReceiver, builder Builder, d Dialect) (string, *sql.Rows, error) {
	// discard the timeout set in the runner, the context should not be canceled
	// implicitly here but explicitly by the caller since the returned *sql.Rows
	// may still listening to the context
	i := interpolator{
		Buffer:       NewBuffer(),
		Dialect:      d,
		IgnoreBinary: true,
	}
	err := i.encodePlaceholder(builder, true)
	query, value := i.String(), i.Value()
	if err != nil {
		return query, nil, log.EventErrKv("dbr.select.interpolate", err, kvs{
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
		return query, nil, log.EventErrKv("dbr.select.load.query", err, kvs{
			"sql":  query,
			"time": strconv.FormatInt(time.Since(startTime).Nanoseconds()/1e6, 10),
		})
	}

	return query, rows, nil
}

func query(ctx context.Context, runner runner, log EventReceiver, builder Builder, d Dialect, dest interface{}, customs ...Custom) (int, error) {
	timeout := runner.GetTimeout()
	if timeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	startTime := time.Now()

	var custom Custom //自定义参数
	if len(customs) > 0 {
		custom = customs[0]
	}

	var err error
	var query, dbcacheDataKey string
	var count int

	//redis缓存数据库数据获取
	if custom.isCache {
		query, err = getSQL(builder, d)
		if err != nil {
			return 0, log.EventErrKv("dbr.select.cache.getSQL", err, kvs{
				"sql":  query,
				"time": strconv.FormatInt(time.Since(startTime).Nanoseconds()/1e6, 10),
			})
		}
		if custom.isCount {
			query = fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS count", query)
		}
		//redis缓存数据Key
		dbcacheDataKey = custom.cacheKey
		if dbcacheDataKey == "" {
			return 0, log.EventErrKv("dbr.select.cache.key", errors.New("cache.key can not be empty"), kvs{
				"sql":  query,
				"time": strconv.FormatInt(time.Since(startTime).Nanoseconds()/1e6, 10),
			})
		}

		//查询redis缓存数据是否存在，不存在则查询数据库
		reply, exist, err := custom.cache.GetBytes(dbcacheDataKey)
		if err != nil {
			_ = log.EventErrKv("dbr.select.cache.get", err, kvs{
				"sql":  query,
				"time": strconv.FormatInt(time.Since(startTime).Nanoseconds()/1e6, 10),
			})
		} else {
			if exist {
				if custom.isCount {
					decoder := gob.NewDecoder(bytes.NewReader(reply)) //创建解密器
					err = decoder.Decode(&count) //解密
					if err != nil {
						_ = log.EventErrKv("dbr.select.cache.decode", err, kvs{
							"sql":  query,
							"time": strconv.FormatInt(time.Since(startTime).Nanoseconds()/1e6, 10),
						})
					} else {
						log.TimingKv("dbr.select.cache", time.Since(startTime).Nanoseconds(), kvs{
							"sql": query,
						})
						return count, nil
					}
				} else {
					decoder := gob.NewDecoder(bytes.NewReader(reply)) //创建解密器
					err = decoder.Decode(dest) //解密
					if err != nil {
						_ = log.EventErrKv("dbr.select.cache.decode", err, kvs{
							"sql":  query,
							"time": strconv.FormatInt(time.Since(startTime).Nanoseconds()/1e6, 10),
						})
					} else {
						count = 1
						v := reflect.ValueOf(dest)
						if v.Kind() != reflect.Ptr || v.IsNil() {
							return 0, log.EventErrKv("dbr.select.cache.decode", ErrInvalidPointer, kvs{
								"sql":  query,
								"time": strconv.FormatInt(time.Since(startTime).Nanoseconds()/1e6, 10),
							})
						}
						v = v.Elem()
						if v.Kind() == reflect.Slice {
							count = v.Len()
						}
						log.TimingKv("dbr.select.cache", time.Since(startTime).Nanoseconds(), kvs{
							"sql": query,
						})
						return count, nil
					}
				}
			}
		}
	}

	var cacheBuffer bytes.Buffer
	var cacheBufferErr bool
	if custom.isCount {
		//获取数据库数据条数
		query, count, err = queryCount(ctx, runner, log, builder, d)
		if err != nil {
			return 0, log.EventErrKv("dbr.select.load.scan", err, kvs{
				"sql":  query,
				"time": strconv.FormatInt(time.Since(startTime).Nanoseconds()/1e6, 10),
			})
		}

		//redis缓存数据编码
		if custom.isCache {
			encoder := gob.NewEncoder(&cacheBuffer) //创建编码器
			err = encoder.Encode(&count)            //编码
			if err != nil {
				cacheBufferErr = true
				_ = log.EventErrKv("dbr.select.cache.encode", err, kvs{
					"sql":  query,
					"time": strconv.FormatInt(time.Since(startTime).Nanoseconds()/1e6, 10),
				})
			}
		}
	} else {
		//获取数据库数据
		var rows *sql.Rows
		query, rows, err = queryRows(ctx, runner, log, builder, d)
		if err != nil {
			return 0, log.EventErrKv("dbr.select.load.scan", err, kvs{
				"sql":  query,
				"time": strconv.FormatInt(time.Since(startTime).Nanoseconds()/1e6, 10),
			})
		}
		count, err = Load(rows, dest)
		if err != nil {
			return 0, log.EventErrKv("dbr.select.load.scan", err, kvs{
				"sql":  query,
				"time": strconv.FormatInt(time.Since(startTime).Nanoseconds()/1e6, 10),
			})
		}

		//redis缓存数据编码
		if custom.isCache {
			encoder := gob.NewEncoder(&cacheBuffer) //创建编码器
			err = encoder.Encode(dest)              //编码
			if err != nil {
				cacheBufferErr = true
				_ = log.EventErrKv("dbr.select.cache.encode", err, kvs{
					"sql":  query,
					"time": strconv.FormatInt(time.Since(startTime).Nanoseconds()/1e6, 10),
				})
			}
		}
	}

	//redis缓存数据保存
	if custom.isCache && !cacheBufferErr {
		_, err = custom.cache.Set(dbcacheDataKey, cacheBuffer.Bytes(), custom.cacheExpire)
		if err != nil {
			_ = log.EventErrKv("dbr.select.cache.set", err, kvs{
				"sql":  query,
				"time": strconv.FormatInt(time.Since(startTime).Nanoseconds()/1e6, 10),
			})
		}
	}

	log.TimingKv("dbr.select", time.Since(startTime).Nanoseconds(), kvs{
		"sql": query,
	})
	return count, nil
}

func queryCount(ctx context.Context, runner runner, log EventReceiver, builder Builder, d Dialect) (string, int, error) {
	i := interpolator{
		Buffer:       NewBuffer(),
		Dialect:      d,
		IgnoreBinary: true,
	}
	err := i.encodePlaceholder(builder, true)
	query, value := i.String(), i.Value()
	query = fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS count", query)
	if err != nil {
		return query, 0, log.EventErrKv("dbr.select.interpolate", err, kvs{
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
		return query, 0, log.EventErrKv("dbr.select.load.query", err, kvs{
			"sql":  query,
			"time": strconv.FormatInt(time.Since(startTime).Nanoseconds()/1e6, 10),
		})
	}
	defer rows.Close()
	var count int
	if rows.Next() {
		rows.Scan(&count)
	}

	return query, count, nil
}

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

//md5加密
func md5Str(str string) string {
	hash := md5.New()
	hash.Write([]byte(str))
	return fmt.Sprintf("%x", hash.Sum(nil))
}
