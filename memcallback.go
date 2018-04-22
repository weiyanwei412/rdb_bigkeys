package main

import (
	//"fmt"
	"sync"
)

type MemCallback struct {
	db       int
	chanSize int

	memCalMap []map[string]*printStruct

	memLock sync.RWMutex

	redisChan chan redisTypeGen
}

func (m *MemCallback) Init() {
	if m.chanSize == 0 {
		m.chanSize = 256
	}

	m.memCalMap = make([]map[string]*printStruct, REDIS_DB_NUMB)
	m.redisChan = make(chan redisTypeGen, m.chanSize)

}

func (m *MemCallback) StartRDB() {
	//fmt.Println("start read rdb")
}

func (m *MemCallback) StartDatabase(n int) {
	//fmt.Println("start db ", n)
	m.db = n

	m.memLock.Lock()
	m.memCalMap[n] = map[string]*printStruct{}
	m.memLock.Unlock()
}

func (m *MemCallback) Aux(key, value []byte) {}

func (m *MemCallback) ResizeDatabase(dbSize, expiresSize uint32) {}

func (m *MemCallback) Set(key, value []byte, expiry int64) {
	////fmt.Println("string set:", string(key), string(value))
	m.redisChan <- redisTypeGen{db: m.db, datType: "string", key: key, expiry: expiry, valueSize: len(value)}
}

func (m *MemCallback) StartHash(key []byte, length, expiry int64) {
	//fmt.Println("hash start:", string(key), length, expiry)
	m.redisChan <- redisTypeGen{db: m.db, datType: "hash", action: "start", key: key, length: length, expiry: expiry}
}

func (m *MemCallback) Hset(key, field, value []byte) {
	//fmt.Println("hash element:", string(key), string(field), string(value))
	m.redisChan <- redisTypeGen{db: m.db, datType: "hash", action: "element", key: key, field: field, valueSize: len(value)}
}

func (m *MemCallback) EndHash(key []byte) {
	//fmt.Println("hash end:", string(key))
	m.redisChan <- redisTypeGen{db: m.db, datType: "hash", action: "end", key: key}
}

func (m *MemCallback) StartSet(key []byte, cardinality, expiry int64) {
	//fmt.Println("set start:", string(key), cardinality, expiry)
	m.redisChan <- redisTypeGen{db: m.db, datType: "set", action: "start", key: key, length: cardinality, expiry: expiry}
}

func (m *MemCallback) Sadd(key, member []byte) {
	//fmt.Println("set element: ", string(key), string(member))
	m.redisChan <- redisTypeGen{db: m.db, datType: "set", action: "element", key: key, valueSize: len(member)}
}

func (m *MemCallback) EndSet(key []byte) {
	//fmt.Println("set end:", string(key))
	m.redisChan <- redisTypeGen{db: m.db, datType: "set", action: "end", key: key}
}

func (m *MemCallback) StartList(key []byte, length, expiry int64) {
	//fmt.Println("List start:", string(key), length, expiry)
	m.redisChan <- redisTypeGen{db: m.db, datType: "list", action: "start", key: key, length: length, expiry: expiry}
}

func (m *MemCallback) Rpush(key, value []byte) {
	//fmt.Println("List element:", string(key), string(value))
	m.redisChan <- redisTypeGen{db: m.db, datType: "list", action: "element", key: key, valueSize: len(value)}
}

func (m *MemCallback) EndList(key []byte) {
	//fmt.Println("List end:", string(key))
	m.redisChan <- redisTypeGen{db: m.db, datType: "list", action: "end", key: key}
}

func (m *MemCallback) StartZSet(key []byte, cardinality, expiry int64) {
	//fmt.Println("zset start:", string(key), cardinality, expiry)
	m.redisChan <- redisTypeGen{db: m.db, key: key, datType: "zset", action: "start", length: cardinality, expiry: expiry}
	//m.setChan <- redisSetType{db: m.db, action: "start", key: key, cardinality: cardinality, expiry: expiry}
}

func (m *MemCallback) Zadd(key []byte, score float64, member []byte) {
	//fmt.Println("zset element:", string(key), score, string(member))
	m.redisChan <- redisTypeGen{db: m.db, key: key, datType: "zset", action: "element", valueSize: len(member) + 8}
	//m.setChan <- redisSetType{db: m.db, action: "element", key: key, member: member}
}

func (m *MemCallback) EndZSet(key []byte) {
	//fmt.Println("zset end:", string(key))
	m.redisChan <- redisTypeGen{db: m.db, datType: "zset", action: "end", key: key}
	//m.setChan <- redisSetType{db: m.db, action: "end", key: key}
}

func (m *MemCallback) EndDatabase(n int) {
	//fmt.Println("end database ", n)
}

func (m *MemCallback) EndRDB() {
	//fmt.Println("end read rdb")
	close(m.redisChan)
}
