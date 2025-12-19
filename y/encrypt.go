/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package y

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"
)

// XORBlock encrypts the given data with AES and XOR's with IV.
// Can be used for both encryption and decryption. IV is of
// AES block size.
// 使用了 AES 加密算法 配合 CTR (Counter) 模式来对block加解密。
// NOTE:下面这个函数既可以用于加密,也可以用于解密
// 注意如果是解密,iv取出待解密块的最后16字节即可.而如果是加密,iv由y.GenerateIV()生成
func XORBlock(dst, src, key, iv []byte) error {
	block, err := aes.NewCipher(key)
	// NOTE:aes.NewCipher：这是 Go 标准库函数。它根据传入 key 的长度（16、24 或 32 字节）自动选择 AES-128、AES-192 或 AES-256 算法。
	// NOTE:Block：此时得到的是一个“分组密码”对象，它只能处理固定长度（16字节）的数据块。
	if err != nil {
		return err
	}

	stream := cipher.NewCTR(block, iv)
	// NOTE:CTR (Counter) 模式：这是一种将“分组密码（Block Cipher）”转换为“流密码（Stream Cipher）”的模式。
	stream.XORKeyStream(dst, src) // 它遍历 src 中的每一个字节，与生成的“密钥流”中的对应字节进行 XOR (异或，符号 ^) 运算，结果写入 dst。
	return nil
}

func XORBlockAllocate(src, key, iv []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	stream := cipher.NewCTR(block, iv)
	dst := make([]byte, len(src))
	stream.XORKeyStream(dst, src)
	return dst, nil
}

func XORBlockStream(w io.Writer, src, key, iv []byte) error {
	block, err := aes.NewCipher(key)
	if err != nil {
		return err
	}
	stream := cipher.NewCTR(block, iv)
	sw := cipher.StreamWriter{S: stream, W: w}
	_, err = io.Copy(sw, bytes.NewReader(src))
	return Wrapf(err, "XORBlockStream")
}

// GenerateIV generates IV.
func GenerateIV() ([]byte, error) {
	iv := make([]byte, aes.BlockSize)
	_, err := rand.Read(iv)
	return iv, err
}
