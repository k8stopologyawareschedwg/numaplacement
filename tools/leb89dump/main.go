// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Red Hat, Inc.

// leb89dump prints the LEB89 alphabet mapping: index -> symbol.
// Useful for hand-computing expected encoded strings when writing tests.
package main

import (
	"fmt"

	"github.com/k8stopologyawareschedwg/numaplacement/leb89"
)

func main() {
	for i := int32(0); i < leb89.AlphabetSize; i++ {
		s := leb89.EncodeIntoString(i)
		kind := "terminal"
		if i >= leb89.NumTerminal {
			kind = "continuation"
		}
		fmt.Printf("%3d  %s  %s\n", i, s, kind)
	}
}
