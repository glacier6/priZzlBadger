/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package options

// ChecksumVerificationMode tells when should DB verify checksum for SSTable blocks.
type ChecksumVerificationMode int

const (
	// NoVerification indicates DB should not verify checksum for SSTable blocks.
	NoVerification ChecksumVerificationMode = iota
	// OnTableRead indicates checksum should be verified while opening SSTtable.
	OnTableRead
	// OnBlockRead indicates checksum should be verified on every SSTable block read.
	OnBlockRead
	// OnTableAndBlockRead indicates checksum should be verified
	// on SSTable opening and on every block read.
	OnTableAndBlockRead
)

// CompressionType specifies how a block should be compressed.
type CompressionType uint32

const (
	// None mode indicates that a block is not compressed.
	// 无模式表示block未被压缩。
	None CompressionType = 0
	// Snappy mode indicates that a block is compressed using Snappy algorithm.
	// Snappy模式表示使用Snappy算法压缩block。
	Snappy CompressionType = 1
	// ZSTD mode indicates that a block is compressed using ZSTD algorithm.
	// ZSTD模式表示使用ZSTD算法压缩block。
	ZSTD CompressionType = 2
)
