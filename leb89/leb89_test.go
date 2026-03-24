// SPDX-License-Identifier: Apache-2.0
// AI-Attribution: AIA PAI Nc Hin R claude-4.6-opus v1.0

package leb89

import (
	"testing"
)

func TestLeb89AlphabetSize(t *testing.T) {
	count := 0
	for _, b := range safeASCII {
		if b != 0 {
			count++
		}
	}
	if count != 89 {
		t.Fatalf("expected 89 safe ASCII characters, got %d", count)
	}
}

func TestLeb89AlphabetExcludesUnsafe(t *testing.T) {
	for _, u := range []byte{'"', '&', '<', '>', '\\'} {
		if asciiIndex[u] != -1 {
			t.Errorf("unsafe character %q (0x%02X) should not be in alphabet, found at index %d", u, u, asciiIndex[u])
		}
	}
}

func TestLeb89AlphabetReverseMap(t *testing.T) {
	for i, b := range safeASCII {
		if asciiIndex[b] != i {
			t.Errorf("asciiIndex[0x%02X] = %d, want %d", b, asciiIndex[b], i)
		}
	}
	// Spot-check a few unmapped bytes
	for _, b := range []byte{0, ' ', '"', '&', '<', '>', '\\', 0x7F} {
		if asciiIndex[b] != -1 {
			t.Errorf("asciiIndex[0x%02X] = %d, want -1", b, asciiIndex[b])
		}
	}
}

func TestLeb89TerminalContinuationSplit(t *testing.T) {
	for i := 0; i < leb89NumTerminal; i++ {
		if safeASCII[i] < '!' || safeASCII[i] > 'e' {
			t.Errorf("terminal index %d is %q, expected '!'..'e'", i, safeASCII[i])
		}
	}
	for i := leb89NumTerminal; i < 89; i++ {
		if safeASCII[i] < 'f' || safeASCII[i] > '~' {
			t.Errorf("continuation index %d is %q, expected 'f'..'~'", i, safeASCII[i])
		}
	}
}

func TestLeb89EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		value int32
		chars int
	}{
		{"zero", 0, 1},
		{"one", 1, 1},
		{"mid_terminal", 32, 1},
		{"max_terminal", 63, 1},
		{"min_two_char", 64, 2},
		{"mid_two_char", 800, 2},
		{"max_two_char", 1663, 2},
		{"min_three_char", 1664, 3},
		{"offset_2047", 2047, 3},
		{"max_three_char", 40063, 3},
		{"min_four_char", 40064, 4},
		{"large_four_char", 100000, 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeIntoBytes(nil, tt.value)
			if len(encoded) != tt.chars {
				t.Errorf("EncodeIntoBytes(%d): got %d chars %q, want %d chars", tt.value, len(encoded), encoded, tt.chars)
			}
			decoded, pos := DecodeFromString(string(encoded), 0)
			if decoded != tt.value {
				t.Errorf("roundtrip(%d): decoded %d", tt.value, decoded)
			}
			if pos != len(encoded) {
				t.Errorf("roundtrip(%d): consumed %d bytes, want %d", tt.value, pos, len(encoded))
			}
		})
	}
}

func TestLeb89RoundTrip(t *testing.T) {
	for v := int32(0); v < 50000; v++ {
		encoded := EncodeIntoBytes(nil, v)
		decoded, pos := DecodeFromString(string(encoded), 0)
		if decoded != v {
			t.Fatalf("roundtrip failed at %d: got %d", v, decoded)
		}
		if pos != len(encoded) {
			t.Fatalf("roundtrip at %d: consumed %d bytes, encoded %d bytes", v, pos, len(encoded))
		}
	}
}

func TestLeb89OutputIsSafeASCII(t *testing.T) {
	for v := int32(0); v < 50000; v++ {
		for _, b := range EncodeIntoBytes(nil, v) {
			if asciiIndex[b] == -1 {
				t.Fatalf("value %d produced byte 0x%02X which is not in the safe alphabet", v, b)
			}
		}
	}
}

func TestLeb89ValueRangeBoundaries(t *testing.T) {
	assertLen := func(t *testing.T, v int32, wantLen int) {
		t.Helper()
		if got := len(EncodeIntoBytes(nil, v)); got != wantLen {
			t.Errorf("value %d: got %d chars, want %d", v, got, wantLen)
		}
	}
	// 1-char: 0..63
	assertLen(t, 0, 1)
	assertLen(t, 63, 1)
	// 2-char: 64..1663
	assertLen(t, 64, 2)
	assertLen(t, 1663, 2)
	// 3-char: 1664..40063
	assertLen(t, 1664, 3)
	assertLen(t, 40063, 3)
	// 4-char: 40064+
	assertLen(t, 40064, 4)
	assertLen(t, 1000063, 4)
}

func TestLeb89EncodeAppends(t *testing.T) {
	buf := []byte("prefix")
	buf = EncodeIntoBytes(buf, 5)
	if string(buf[:6]) != "prefix" {
		t.Fatalf("EncodeIntoBytes corrupted existing prefix: %q", buf)
	}
	if len(buf) != 7 {
		t.Fatalf("expected length 7, got %d", len(buf))
	}
}

func TestLeb89DecodeMultipleValues(t *testing.T) {
	values := []int32{0, 7, 63, 64, 500, 1663, 1664, 2047, 40063, 40064}
	var buf []byte
	for _, v := range values {
		buf = EncodeIntoBytes(buf, v)
	}

	s := string(buf)
	pos := 0
	for i, want := range values {
		got, newPos := DecodeFromString(s, pos)
		if got != want {
			t.Fatalf("value[%d]: got %d, want %d", i, got, want)
		}
		pos = newPos
	}
	if pos != len(s) {
		t.Errorf("did not consume all bytes: consumed %d of %d", pos, len(s))
	}
}

func TestLeb89DecodeEmpty(t *testing.T) {
	v, pos := DecodeFromString("", 0)
	if v != -1 {
		t.Errorf("empty string: got value %d, want -1", v)
	}
	if pos != 0 {
		t.Errorf("empty string: got pos %d, want 0", pos)
	}
}

func TestLeb89DecodeMidString(t *testing.T) {
	// Encode two values, decode starting from the second one
	var buf []byte
	buf = EncodeIntoBytes(buf, 42)
	firstLen := len(buf)
	buf = EncodeIntoBytes(buf, 99)

	s := string(buf)
	v, pos := DecodeFromString(s, firstLen)
	if v != 99 {
		t.Errorf("mid-string decode: got %d, want 99", v)
	}
	if pos != len(s) {
		t.Errorf("mid-string decode: consumed to %d, want %d", pos, len(s))
	}
}

func BenchmarkLeb89Encode(b *testing.B) {
	buf := make([]byte, 0, 16)
	for _, bc := range []struct {
		name string
		val  int32
	}{
		{"1char", 42},
		{"2char", 500},
		{"3char", 2047},
	} {
		b.Run(bc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = EncodeIntoBytes(buf[:0], bc.val)
			}
		})
	}
}

func BenchmarkLeb89Decode(b *testing.B) {
	for _, bc := range []struct {
		name string
		val  int32
	}{
		{"1char", 42},
		{"2char", 500},
		{"3char", 2047},
	} {
		s := string(EncodeIntoBytes(nil, bc.val))
		b.Run(bc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				DecodeFromString(s, 0)
			}
		})
	}
}
