package store

import "os"
import "fmt"
import "github.com/tecbot/gorocksdb"

type StoreObject struct {
    path string
    config *gorocksdb.Options
    rocksdb *gorocksdb.DB

    wo *gorocksdb.WriteOptions
    ro *gorocksdb.ReadOptions

}

func NewStoreObject(path string, config *gorocksdb.Options) *StoreObject {
    var dbpath string
    if dbpath == "" {
        dbpath = "./db"
    } else {
        dbpath = path
    }

    var opts *gorocksdb.Options
    if config == nil {
        bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
        bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))

        opts = gorocksdb.NewDefaultOptions()
        opts.SetBlockBasedTableFactory(bbto)
        opts.SetCreateIfMissing(true)
    } else {
        opts = config
    }

    db, err := gorocksdb.OpenDb(opts, dbpath)
    if err != nil {
        fmt.Printf("Make gorocksdb error: %q", err)
        os.Exit(1)
    }

    wo := gorocksdb.NewDefaultWriteOptions()
    ro := gorocksdb.NewDefaultReadOptions()

    return &StoreObject{
        path: dbpath, 
        config: opts, 
        rocksdb: db, 
        wo: wo, 
        ro: ro, 
    }
}

func (s *StoreObject) Put(key string, value string) (err error) {
    err = s.rocksdb.Put(s.wo, []byte(key), []byte(value))
    
    return err
}

func (s *StoreObject) Get(key string) (value string, err error) {
    slice, err := s.rocksdb.Get(s.ro, []byte(key))
    value = string(slice.Data())
    defer slice.Free()

    return value, err
}

func (s *StoreObject) Delete(key string) (err error) {
    err = s.rocksdb.Delete(s.wo, []byte(key))

    return err
}

