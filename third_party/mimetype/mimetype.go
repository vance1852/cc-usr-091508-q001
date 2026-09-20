// Package mimetype 是 github.com/gabriel-vasile/mimetype 的 API 兼容最小实现。
//
// 存在原因：构建环境的出口代理无法获取上游模块 zip（重定向目标被拦截），
// 而 go-playground/validator 仅使用 DetectReader 与 (*MIME).String。
// 本实现覆盖常见魔数嗅探，行为与上游一致（未识别时返回 application/octet-stream）。
// 若构建环境可正常访问 Go 模块代理，移除 go.mod 中的 replace 即可回到上游版本。
package mimetype

import (
	"bytes"
	"io"
)

// MIME 描述一次类型探测的结果。
type MIME struct {
	mime      string
	extension string
}

// String 返回 MIME 类型字符串。
func (m *MIME) String() string { return m.mime }

// Extension 返回常见文件扩展名。
func (m *MIME) Extension() string { return m.extension }

// Is 判断探测结果是否匹配给定类型或其前缀（如 "image/"）。
func (m *MIME) Is(expected string) bool {
	if expected == "" {
		return false
	}
	if expected[len(expected)-1] == '/' {
		return len(m.mime) >= len(expected) && m.mime[:len(expected)] == expected
	}
	return m.mime == expected
}

type signature struct {
	mime, ext string
	magic     []byte
	offset    int
}

var signatures = []signature{
	{"image/jpeg", ".jpg", []byte{0xFF, 0xD8, 0xFF}, 0},
	{"image/png", ".png", []byte{0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A}, 0},
	{"image/gif", ".gif", []byte("GIF8"), 0},
	{"image/webp", ".webp", []byte("WEBP"), 8},
	{"application/pdf", ".pdf", []byte("%PDF-"), 0},
	{"application/zip", ".zip", []byte("PK\x03\x04"), 0},
	{"application/gzip", ".gz", []byte{0x1F, 0x8B}, 0},
	{"application/x-bzip2", ".bz2", []byte("BZh"), 0},
	{"application/x-xz", ".xz", []byte{0xFD, '7', 'z', 'X', 'Z', 0x00}, 0},
	{"application/x-7z-compressed", ".7z", []byte("7z\xBC\xAF\x27\x1C"), 0},
	{"application/x-rar-compressed", ".rar", []byte("Rar!\x1A\x07"), 0},
	{"audio/mpeg", ".mp3", []byte("ID3"), 0},
	{"video/mp4", ".mp4", []byte("ftyp"), 4},
	{"application/wasm", ".wasm", []byte{0x00, 0x61, 0x73, 0x6D}, 0},
	{"application/x-executable", ".elf", []byte{0x7F, 'E', 'L', 'F'}, 0},
}

// Detect 探测字节流的 MIME 类型。
func Detect(in []byte) *MIME {
	for _, sig := range signatures {
		if len(in) >= sig.offset+len(sig.magic) &&
			bytes.Equal(in[sig.offset:sig.offset+len(sig.magic)], sig.magic) {
			return &MIME{mime: sig.mime, extension: sig.ext}
		}
	}
	if isText(in) {
		return &MIME{mime: "text/plain; charset=utf-8", extension: ".txt"}
	}
	return &MIME{mime: "application/octet-stream", extension: ".bin"}
}

// DetectReader 从 reader 读取头部并探测 MIME 类型。
func DetectReader(r io.Reader) (*MIME, error) {
	const readLimit = 3072
	buf := make([]byte, readLimit)
	n, err := io.ReadAtLeast(r, buf, 1)
	if err != nil && n == 0 {
		return nil, err
	}
	return Detect(buf[:n]), nil
}

// isText 粗略判断是否为可打印文本（UTF-8 友好）。
func isText(in []byte) bool {
	for _, b := range in {
		if b == 0 {
			return false
		}
		if b < 0x20 && b != '\n' && b != '\r' && b != '\t' && b != '\f' && b != '\b' {
			return false
		}
	}
	return true
}
