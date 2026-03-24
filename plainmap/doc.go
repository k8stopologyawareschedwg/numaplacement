// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Red Hat, Inc.

// plainmap package implements the simplest possible
// numalocality representation: per-numa container set represented literally,
// passing through strings to represent container placement vectors.
// It as test oracle and benchmarking baseline.
// Should never be used in production, this is what the  main package is for.
package plainmap
