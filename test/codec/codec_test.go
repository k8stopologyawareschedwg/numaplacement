// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Red Hat, Inc.

package integration

import (
	"reflect"
	"testing"

	"github.com/k8stopologyawareschedwg/numaplacement"
)

func containerIDsFromAffinities(affs []numaplacement.ContainerAffinity) []numaplacement.ContainerID {
	ids := make([]numaplacement.ContainerID, len(affs))
	for i, a := range affs {
		ids[i] = a.ID
	}
	return ids
}

func assertInfoMatchesAffinities(t *testing.T, info numaplacement.Info, affs []numaplacement.ContainerAffinity) {
	t.Helper()
	for _, a := range affs {
		n, err := info.NUMAAffinity(a.ID)
		if err != nil {
			t.Fatalf("NUMAAffinity(%s): %v", a.ID.String(), err)
		}
		if n != a.NUMANode {
			t.Fatalf("NUMAAffinity(%s): got NUMA %d, want %d", a.ID.String(), n, a.NUMANode)
		}
	}
}

func TestEncoderDecoder_ZeroContainers(t *testing.T) {
	cases := []struct {
		name      string
		numaNodes int
	}{
		{"one NUMA node", 1},
		{"two NUMA nodes", 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expectedPl := numaplacement.Payload{
				Containers:     0,
				NUMANodes:      tc.numaNodes,
				BusiestNode:    0,
				VectorEncoding: numaplacement.VectorEncodingLEB89,
				Vectors:        make(map[int]string),
			}
			if err := expectedPl.Validate(func(numaplacement.Payload) error { return nil }); err != nil {
				t.Fatalf("Validate expectedPl: %v", err)
			}

			enc, err := numaplacement.NewEncoder(tc.numaNodes)
			if err != nil {
				t.Fatalf("NewEncoder: %v", err)
			}
			encPl, err := enc.Result()
			if err != nil {
				t.Fatalf("Encoder.Result: %v", err)
			}
			if !reflect.DeepEqual(encPl, expectedPl) {
				t.Fatalf("encoded payload:\n got %#v\nwant %#v", encPl, expectedPl)
			}

			dec, err := numaplacement.NewDecoder(encPl)
			if err != nil {
				t.Fatalf("NewDecoder: %v", err)
			}
			info, err := dec.Result()
			if err != nil {
				t.Fatalf("Decoder.Result: %v", err)
			}
			if info.Containers() != 0 {
				t.Fatalf("decoded container count: got %d, want 0", info.Containers())
			}
		})
	}
}

func TestEncoderDecoder_SingleNUMANodeZeroVectors(t *testing.T) {
	affs := []numaplacement.ContainerAffinity{
		{
			ID: numaplacement.ContainerID{
				Namespace:     "default",
				PodName:       "workload-a",
				ContainerName: "app",
			},
			NUMANode: 0,
		},
		{
			ID: numaplacement.ContainerID{
				Namespace:     "default",
				PodName:       "workload-a",
				ContainerName: "sidecar",
			},
			NUMANode: 0,
		},
	}

	cases := []struct {
		name      string
		numaNodes int
	}{
		{"one NUMA node", 1},
		{"two NUMA nodes", 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expectedPl := numaplacement.Payload{
				Containers:     2,
				NUMANodes:      tc.numaNodes,
				BusiestNode:    0,
				VectorEncoding: numaplacement.VectorEncodingLEB89,
				Vectors:        make(map[int]string),
			}
			if err := expectedPl.Validate(func(numaplacement.Payload) error { return nil }); err != nil {
				t.Fatalf("Validate expectedPl: %v", err)
			}

			enc, err := numaplacement.NewEncoder(tc.numaNodes, affs...)
			if err != nil {
				t.Fatalf("NewEncoder: %v", err)
			}
			encPl, err := enc.Result()
			if err != nil {
				t.Fatalf("Encoder.Result: %v", err)
			}
			if !reflect.DeepEqual(encPl, expectedPl) {
				t.Fatalf("encoded payload:\n got %#v\nwant %#v", encPl, expectedPl)
			}

			dec, err := numaplacement.NewDecoder(encPl, containerIDsFromAffinities(affs)...)
			if err != nil {
				t.Fatalf("NewDecoder: %v", err)
			}
			info, err := dec.Result()
			if err != nil {
				t.Fatalf("Decoder.Result: %v", err)
			}
			assertInfoMatchesAffinities(t, info, affs)
		})
	}
}

func TestEncoderDecoder_MultipleNUMANodes_EvenDistribution(t *testing.T) {
	affs := []numaplacement.ContainerAffinity{
		{
			ID: numaplacement.ContainerID{
				Namespace:     "default",
				PodName:       "workload-a",
				ContainerName: "app1",
			},
			NUMANode: 0,
		},
		{
			ID: numaplacement.ContainerID{
				Namespace:     "default",
				PodName:       "workload-a",
				ContainerName: "sidecar2",
			},
			NUMANode: 0,
		},
		{
			ID: numaplacement.ContainerID{
				Namespace:     "default",
				PodName:       "workload-a",
				ContainerName: "sidecar3",
			},
			NUMANode: 1,
		},
		{
			ID: numaplacement.ContainerID{
				Namespace:     "default",
				PodName:       "workload-a",
				ContainerName: "sidecar4",
			},
			NUMANode: 1,
		},
	}

	enc, err := numaplacement.NewEncoder(2, affs...)
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	encPl, err := enc.Result()
	if err != nil {
		t.Fatalf("Encoder.Result: %v", err)
	}
	if encPl.Containers != 4 || encPl.NUMANodes != 2 {
		t.Fatalf("payload metadata: got containers=%d numaNodes=%d", encPl.Containers, encPl.NUMANodes)
	}
	if encPl.BusiestNode != 0 {
		t.Fatalf("BusiestNode on tie: got %d, want 0 (lowest index wins)", encPl.BusiestNode)
	}
	if encPl.VectorEncoding != numaplacement.VectorEncodingLEB89 {
		t.Fatalf("VectorEncoding: got %q", encPl.VectorEncoding)
	}
	if len(encPl.Vectors) != 1 {
		t.Fatalf("Vectors len: got %d, want 1", len(encPl.Vectors))
	}
	if _, ok := encPl.Vectors[1]; !ok {
		t.Fatalf("Vectors should encode non-busiest node 1 only; got %#v", encPl.Vectors)
	}

	dec, err := numaplacement.NewDecoder(encPl, containerIDsFromAffinities(affs)...)
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}
	info, err := dec.Result()
	if err != nil {
		t.Fatalf("Decoder.Result: %v", err)
	}
	if info.Containers() != len(affs) {
		t.Fatalf("container count: got %d, want %d", info.Containers(), len(affs))
	}
	assertInfoMatchesAffinities(t, info, affs)
}

func TestEncoderDecoder_MultipleNUMANodes_UnevenDistribution(t *testing.T) {
	affs := []numaplacement.ContainerAffinity{
		{
			ID: numaplacement.ContainerID{
				Namespace:     "default",
				PodName:       "workload-a",
				ContainerName: "app1",
			},
			NUMANode: 0,
		},
		{
			ID: numaplacement.ContainerID{
				Namespace:     "default",
				PodName:       "workload-a",
				ContainerName: "sidecar2",
			},
			NUMANode: 0,
		},
		{
			ID: numaplacement.ContainerID{
				Namespace:     "default",
				PodName:       "workload-a",
				ContainerName: "sidecar3",
			},
			NUMANode: 0,
		},
		{
			ID: numaplacement.ContainerID{
				Namespace:     "default",
				PodName:       "workload-a",
				ContainerName: "sidecar4",
			},
			NUMANode: 1,
		},
	}

	enc, err := numaplacement.NewEncoder(3, affs...)
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}
	encPl, err := enc.Result()
	if err != nil {
		t.Fatalf("Encoder.Result: %v", err)
	}
	if encPl.BusiestNode != 0 {
		t.Fatalf("BusiestNode: got %d, want 0", encPl.BusiestNode)
	}
	if len(encPl.Vectors) != 1 {
		t.Fatalf("Vectors len: got %d, want 1", len(encPl.Vectors))
	}
	if _, ok := encPl.Vectors[1]; !ok {
		t.Fatalf("Vectors should include node 1 only; got %#v", encPl.Vectors)
	}

	dec, err := numaplacement.NewDecoder(encPl, containerIDsFromAffinities(affs)...)
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}
	info, err := dec.Result()
	if err != nil {
		t.Fatalf("Decoder.Result: %v", err)
	}
	if info.Containers() != len(affs) {
		t.Fatalf("container count: got %d, want %d", info.Containers(), len(affs))
	}
	assertInfoMatchesAffinities(t, info, affs)
}
