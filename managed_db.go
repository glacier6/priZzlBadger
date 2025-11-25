/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

// OpenManaged returns a new DB, which allows more control over setting
// transaction timestamps, aka managed mode.
//
// This is only useful for databases built on top of Badger (like Dgraph), and
// can be ignored by most users.
// OpenManaged返回一个新的DB，它允许对设置事务时间戳进行更多控制，也称为托管模式。
// 这仅适用于基于Badger构建的数据库（如Dgraph），大多数用户可以忽略它。
// NOTE:2025112500
func OpenManaged(opts Options) (*DB, error) {
	opts.managedTxns = true // 设置托管模式
	return Open(opts)
}

// NewTransactionAt follows the same logic as DB.NewTransaction(), but uses the
// provided read timestamp.
//
// This is only useful for databases built on top of Badger (like Dgraph), and
// can be ignored by most users.
// NewTransactionAt 遵循与 DB.NewTransaction () 相同的逻辑，但其使用的是传入的读取时间戳。
// 该函数仅对基于 Badger 构建的数据库（如 Dgraph）有用，大多数用户可忽略此函数。
// NOTE:2025112501 Dgraph中创建事务
func (db *DB) NewTransactionAt(readTs uint64, update bool) *Txn {
	if !db.opt.managedTxns {
		// 一般只有托管模式才可以用at创建事务
		panic("Cannot use NewTransactionAt with managedDB=false. Use NewTransaction instead.")
	}
	txn := db.newTransaction(update, true) // NOTE:核心操作，先创建一个事务
	txn.readTs = readTs                    // 然后直接赋予readTs
	return txn
}

// NewWriteBatchAt is similar to NewWriteBatch but it allows user to set the commit timestamp.
// NewWriteBatchAt is supposed to be used only in the managed mode.
func (db *DB) NewWriteBatchAt(commitTs uint64) *WriteBatch {
	if !db.opt.managedTxns {
		panic("cannot use NewWriteBatchAt with managedDB=false. Use NewWriteBatch instead")
	}

	wb := db.newWriteBatch(true)
	wb.commitTs = commitTs
	wb.txn.commitTs = commitTs
	return wb
}
func (db *DB) NewManagedWriteBatch() *WriteBatch {
	if !db.opt.managedTxns {
		panic("cannot use NewManagedWriteBatch with managedDB=false. Use NewWriteBatch instead")
	}

	wb := db.newWriteBatch(true)
	return wb
}

// CommitAt commits the transaction, following the same logic as Commit(), but
// at the given commit timestamp. This will panic if not used with managed transactions.
//
// This is only useful for databases built on top of Badger (like Dgraph), and
// can be ignored by most users.
// CommitAt 提交事务，遵循与 Commit () 相同的逻辑，但使用给定的提交时间戳
// 若未与托管事务配合使用，此操作将引发 panic
// 此功能仅对基于 Badger 构建的数据库（如 Dgraph）有用，大多数用户可忽略此函数
// NOTE:2025112503 Dgraph事务提交
func (txn *Txn) CommitAt(commitTs uint64, callback func(error)) error {
	if !txn.db.opt.managedTxns {
		panic("Cannot use CommitAt with managedDB=false. Use Commit instead.")
	}
	txn.commitTs = commitTs
	if callback == nil {
		return txn.Commit()
	}
	txn.CommitWith(callback)
	return nil
}

// SetDiscardTs sets a timestamp at or below which, any invalid or deleted
// versions can be discarded from the LSM tree, and thence from the value log to
// reclaim disk space. Can only be used with managed transactions.
// ``` SetDiscardTs 设置一个时间戳，低于该时间戳的任何无效或已删除版本可以从LSM树中移除，进而从值日志中删除以回收磁盘空间。仅限于管理型事务使用。```。
func (db *DB) SetDiscardTs(ts uint64) {
	if !db.opt.managedTxns {
		panic("Cannot use SetDiscardTs with managedDB=false.")
	}
	db.orc.setDiscardTs(ts)
}
