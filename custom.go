package dbr

//自定义参数
type Custom struct {
    isCount     bool        //是否获取总条数
    isCache     bool        //是否redis缓存
    cache       customCache //redis缓存
    cacheKey    string      //redis缓存key
    cacheExpire int64       //redis缓存时间
}

//redis缓存工具对象
type customCache interface {
    Set(key string, value interface{}, seconds ...int64) (old string, err error)
    GetBytes(key string) (reply []byte, exists bool, err error)
    Del(key string) error
}
