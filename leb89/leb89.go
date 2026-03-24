// SPDX-License-Identifier: Apache-2.0
// AI-Attribution: AIA PAI Nc Hin R claude-4.6-opus v1.0

package leb89

const (
	Unmapped int32 = -1
)

const (
	leb89NumTerminal     = 64
	leb89NumContinuation = 25 // 89 - 64
)

// safeASCII holds the 89 printable ASCII characters that Go's json.Marshal
// passes through without escaping: all of 0x21-0x7E except " & < > \
var safeASCII [89]byte

// asciiIndex is the reverse map: byte -> index in safeASCII, -1 if unmapped.
var asciiIndex [128]int

func init() {
	idx := 0
	for b := byte(0x21); b <= 0x7E; b++ {
		if b == '"' || b == '&' || b == '<' || b == '>' || b == '\\' {
			continue
		}
		safeASCII[idx] = b
		idx++
	}
	for i := range asciiIndex {
		asciiIndex[i] = int(Unmapped)
	}
	for i, b := range safeASCII {
		asciiIndex[b] = i
	}
}

// EncodeIntoBytes encodes v >= 0 as a LEB89 byte sequence appended to dst.
// Values 0-63: 1 byte (terminal).
// Values 64-1663: 2 bytes (1 continuation + terminal).
// Values 1664-40063: 3 bytes (2 continuations + terminal).
func EncodeIntoBytes(dst []byte, v int32) []byte {
	if v < int32(leb89NumTerminal) {
		return append(dst, safeASCII[v])
	}
	v -= int32(leb89NumTerminal)

	terminal := v % int32(leb89NumTerminal)
	v /= int32(leb89NumTerminal)

	var digits [8]int32
	n := 0
	for {
		digits[n] = v % int32(leb89NumContinuation)
		n++
		v /= int32(leb89NumContinuation)
		if v == 0 {
			break
		}
	}
	for i := n - 1; i >= 0; i-- {
		dst = append(dst, safeASCII[leb89NumTerminal+digits[i]])
	}
	return append(dst, safeASCII[terminal])
}

// DecodeFromString reads one LEB89-encoded value from s[pos:].
// Returns (value, newPos). Returns (Unmapped, pos) on malformed input.
func DecodeFromString(s string, pos int) (int32, int) {
	var contValue int32
	nCont := 0
	for pos < len(s) {
		idx := asciiIndex[s[pos]]
		pos++
		if idx < leb89NumTerminal {
			if nCont == 0 {
				return int32(idx), pos
			}
			return int32(leb89NumTerminal) + contValue*int32(leb89NumTerminal) + int32(idx), pos
		}
		contValue = contValue*int32(leb89NumContinuation) + int32(idx-leb89NumTerminal)
		nCont++
	}
	return Unmapped, pos
}
