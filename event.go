package dbr

import (
	"context"
	"fmt"
	"time"
)

// EventReceiver gets events from dbr methods for logging purposes.
type EventReceiver interface {
	Event(eventName string)
	EventKv(eventName string, kvs map[string]string)
	EventErr(eventName string, err error) error
	EventErrKv(eventName string, err error, kvs map[string]string) error
	Timing(eventName string, nanoseconds int64)
	TimingKv(eventName string, nanoseconds int64, kvs map[string]string)
}

// TracingEventReceiver is an optional interface an EventReceiver type can implement
// to allow tracing instrumentation
type TracingEventReceiver interface {
	SpanStart(ctx context.Context, eventName, query string) context.Context
	SpanError(ctx context.Context, err error)
	SpanFinish(ctx context.Context)
}

var showSQLLevel int

//是否打印SQL
// level 0：不打印SQL；1：只打印err；2：打印全部
func ShowSQL(level int) {
	showSQLLevel = level
}

type kvs map[string]string

var nullReceiver = &NullEventReceiver{}

// NullEventReceiver is a sentinel EventReceiver.
// Use it if the caller doesn't supply one.
type NullEventReceiver struct{}

// Event receives a simple notification when various events occur.
func (n *NullEventReceiver) Event(eventName string) {}

// EventKv receives a notification when various events occur along with
// optional key/value data.
func (n *NullEventReceiver) EventKv(eventName string, kvs map[string]string) {}

// EventErr receives a notification of an error if one occurs.
func (n *NullEventReceiver) EventErr(eventName string, err error) error { return err }

// EventErrKv receives a notification of an error if one occurs along with
// optional key/value data.
func (n *NullEventReceiver) EventErrKv(eventName string, err error, kvs map[string]string) error {
	if showSQLLevel >= 1 {
		var sql, tim string
		if s, ok := kvs["sql"]; ok {
			sql = s
		}
		if s, ok := kvs["time"]; ok {
			tim = s
		} else {
			tim = "-"
		}
		fmt.Println(fmt.Sprintf("[DBR]%s [ERR %sms] [%v] %s", time.Now().Format("2006/01/02 15:04:05.000"), tim, err, sql))
	}
	return err
}

// Timing receives the time an event took to happen.
func (n *NullEventReceiver) Timing(eventName string, nanoseconds int64) {}

// TimingKv receives the time an event took to happen along with optional key/value data.
func (n *NullEventReceiver) TimingKv(eventName string, nanoseconds int64, kvs map[string]string) {
	if showSQLLevel >= 2 {
		var sql string
		if s, ok := kvs["sql"]; ok {
			sql = s
		}
		fmt.Println(fmt.Sprintf("[DBR]%s [OK %dms] %s", time.Now().Format("2006/01/02 15:04:05.000"), nanoseconds/1e6, sql))
	}
}
