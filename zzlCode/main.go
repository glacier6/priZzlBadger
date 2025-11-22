package main

import (
	"fmt"
	"log"
	"strings"
	"unicode"

	badger "github.com/dgraph-io/badger/v4"
)

func main() {
	// NOTE:下面是通用的一些方法，总共需要看6个部分

	// 1.打开DB（DB初始化）
	db, err := badger.Open(badger.DefaultOptions("/home/hanjiang/DB-CODE/ZZLdgraph/dgraph/p"))
	// db, err := badger.Open(badger.DefaultOptions("./"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 2.读写事务
	// 在读写事务中允许所有数据库操作。
	// err = db.Update(func(txn *badger.Txn) error {
	// 	txn.Set([]byte("answer"), []byte("42"))
	// 	txn.Get([]byte("answer"))

	// 	// 或者下面这种set方式
	// 	e := badger.NewEntry([]byte("answer2"), []byte("55"))
	// 	err := txn.SetEntry(e)
	// 	return err
	// })
	// 3.只读事务
	// 您不能在此事务中执行任何写入或删除。Badger 确保您在此闭包中获得一致的数据库视图。事务开始后在其他地方发生的任何写入, 都不会被闭包内的调用看到。
	err = db.View(func(txn *badger.Txn) error {
		txn.Get([]byte("answer"))
		return nil
	})

	// 4.遍历keys（范围查询），貌似这个代码块没有设置前缀，所以，会把所有数据都返回，然后在下面这个函数参数内进行全部遍历
	err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100 //指定预读取的kv对个数

		// opts.PrefetchValues = false // NOTE:注意上面那一行删掉，然后本行打开，就会变成仅键迭代模式，具体如下
		// Badger 支持一种独特的迭代模式, 称为key-only迭代。它比常规迭代快几个数量级, 因为它只涉及对 LSM 树的访问, 它通常完全驻留在 RAM 中。要启用仅键迭代, 您需要将该IteratorOptions.PrefetchValues 字段设置为false.
		// 这也可用于在迭代期间对选定键进行稀疏读取, item.Value()仅在需要时调用。

		it := txn.NewIterator(opts) //NOTE:核心操作，这个顶级迭代器屏蔽了数据可能在内存，也可能在外存，也可能在事务中，统一进行遍历
		defer it.Close()
		fmt.Printf("| %-*s | %s |\n", 40, "KEY", "VALUE")
		fmt.Printf("|%s|%s|\n", strings.Repeat("-", 40+2), strings.Repeat("-", 20+2))

		// NOTE:综合迭代器的item的版本号都是通过各个子迭代器的key()函数得到的
		// PendingWrites子迭代器的版本直接就是当前事务readTs
		// 其他的诸如memtable什么的SST就是直接是拼接存储时的key+commitTs
		for it.Rewind(); it.Valid(); it.Next() { // Rewind把指针指向遍历的初始位置以及一些初始操作（核心函数），Valid判断当前kv是否有效，Next将指针指向下一个kv
			item := it.Item() // 取出当前遍历器指向的kv
			k := item.Key()
			version := item.Version()
			err := item.Value(func(v []byte) error {
				fmt.Printf("| %-*s | %s \n", 40, filterAlphanumeric(k), filterAlphanumeric(v))
				fmt.Print("ASCII编码下的key为")
				fmt.Print(k) // 注意用的是ascii编码
				fmt.Print("，value为")
				fmt.Print(v)
				fmt.Print("，版本号（commitTs）为 ")
				fmt.Print(version)
				fmt.Printf("\n")
				fmt.Printf("\n")
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	// 5.vlog 的GC
	// err = db.RunValueLogGC(0.7) //脏键百分比0.7
	// _ = err

	//	NOTE:下面是另一种实现事务查询的方式

	// 另外，DB.View()和DB.Update()方法可以被DB.NewTransaction()和Txn.Commit()方法封装 (或者在只读事务下使用Txn.Discard())
	// 这些辅助方法将启动事务, 执行一个函数, 然后在返回错误时安全地丢弃您的事务。这是使用 Badger 交易的推荐方式。
	// DB.NewTransaction(), 接受一个布尔参数来指定是否需要读写事务。
	// 对于读写事务, 需要调用Txn.Commit() 以确保事务被提交。
	// 对于只读事务, 调用 Txn.Discard()就足够了。
	// Txn.Commit()也在内部调用Txn.Discard()以清理事务, 因此只需调用即可Txn.Commit()完成读写事务。
	// 但是, 如果由于某种原因没有调用 Txn.Commit()(例如, 它过早地返回错误), 那么请确保您Txn.Discard()在一个defer块中调用, 如:

	// Start a writable transaction.
	// txn := db.NewTransaction(true)
	// defer txn.Discard()
	// if err1 := txn.Set([]byte("answer"), []byte("42")); err1 != nil {
	// 	//抛出错误
	// }
	// if err2 := txn.Commit(); err2 != nil {
	// 	//抛出错误
	// }

	// 6.LSM日志合并
	// NOTE:2025060500
}

// formatKV 格式化一维字节切片的 KV 对
func cleanString(b []byte) string {
	str := string(b)
	str = strings.TrimSpace(str)             // 去除首尾空白
	str = strings.ReplaceAll(str, "\t", " ") // 替换制表符为空格
	str = strings.ReplaceAll(str, "\"", "")  // 去除引号
	str = strings.ReplaceAll(str, "\n", " ") // 替换换行符为空格
	return str
}
func filterAlphanumeric(bytes []byte) string {
	var result []rune
	for _, b := range bytes {
		// 判断是否为字母或数字（Unicode 范围）
		if unicode.IsLetter(rune(b)) || unicode.IsDigit(rune(b)) ||
			b == '.' || b == '-' || b == '_' {
			result = append(result, rune(b))
		}
	}
	return string(result)
}
