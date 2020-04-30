package dbr

//redis缓存工具对象
type selectCache interface {
    Set(key string, value interface{}, seconds ...int64) (old string, err error)
    GetBytes(key string) (reply []byte, exists bool, err error)
}
