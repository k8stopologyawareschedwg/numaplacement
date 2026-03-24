// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Red Hat, Inc.

package numaplacement

import (
	"errors"
	"testing"
)

func TestPackUnpackMetadataRoundtrip(t *testing.T) {
	tests := []struct {
		name        string
		containers  int
		numaNodes   int
		busiestNode int
	}{
		{name: "all zeros", containers: 0, numaNodes: 0, busiestNode: 0},
		{name: "single container single NUMA", containers: 1, numaNodes: 1, busiestNode: 0},
		{name: "typical two NUMA", containers: 4, numaNodes: 2, busiestNode: 1},
		{name: "large values", containers: 128, numaNodes: 8, busiestNode: 7},
		{name: "busiest is first node", containers: 10, numaNodes: 4, busiestNode: 0},
		{name: "busiest is last node", containers: 10, numaNodes: 4, busiestNode: 3},
		{name: "negative busiest node", containers: 5, numaNodes: 2, busiestNode: -1},
		{name: "all negative", containers: -1, numaNodes: -1, busiestNode: -1},
		{name: "mixed negative positive", containers: -1, numaNodes: 2, busiestNode: 0},
		{name: "large container count", containers: 10000, numaNodes: 2, busiestNode: 1},
		{name: "many NUMA nodes", containers: 16, numaNodes: 64, busiestNode: 32},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orig := Payload{
				Containers:  tt.containers,
				NUMANodes:   tt.numaNodes,
				BusiestNode: tt.busiestNode,
			}
			packed := orig.PackMetadata()

			var got Payload
			if err := UnpackMetadataInto(&got, packed); err != nil {
				t.Fatalf("UnpackMetadataInto() error = %v", err)
			}
			if got.Containers != tt.containers {
				t.Errorf("Containers = %d, want %d", got.Containers, tt.containers)
			}
			if got.NUMANodes != tt.numaNodes {
				t.Errorf("NUMANodes = %d, want %d", got.NUMANodes, tt.numaNodes)
			}
			if got.BusiestNode != tt.busiestNode {
				t.Errorf("BusiestNode = %d, want %d", got.BusiestNode, tt.busiestNode)
			}
		})
	}
}

func TestPackMetadata(t *testing.T) {
	tests := []struct {
		name     string
		payload  Payload
		expected string
	}{
		{
			name:     "zeros",
			payload:  Payload{Containers: 0, NUMANodes: 0, BusiestNode: 0},
			expected: "npv0v001::cc=0::nn=0::bn=0",
		},
		{
			name:     "typical values",
			payload:  Payload{Containers: 4, NUMANodes: 2, BusiestNode: 1},
			expected: "npv0v001::cc=4::nn=2::bn=1",
		},
		{
			name:     "negative values",
			payload:  Payload{Containers: -1, NUMANodes: -1, BusiestNode: -1},
			expected: "npv0v001::cc=-1::nn=-1::bn=-1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.payload.PackMetadata()
			if got != tt.expected {
				t.Errorf("PackMetadata() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestUnpackMetadataIntoErrors(t *testing.T) {
	tests := []struct {
		name     string
		metadata string
		wantErr  error
	}{
		{
			name:     "empty string",
			metadata: "",
			wantErr:  ErrMalformedMetadata,
		},
		{
			name:     "wrong prefix",
			metadata: "XXXXv001::cc=1::nn=2::bn=0",
			wantErr:  ErrMalformedMetadata,
		},
		{
			name:     "wrong version",
			metadata: "npv0v999::cc=1::nn=2::bn=0",
			wantErr:  ErrMalformedMetadata,
		},
		{
			name:     "missing separator after prefix",
			metadata: "npv0v001cc=1::nn=2::bn=0",
			wantErr:  ErrMalformedMetadata,
		},
		{
			name:     "unknown field name",
			metadata: "npv0v001::cc=1::nn=2::xx=0",
			wantErr:  ErrUnknownMetadata,
		},
		{
			name:     "non-numeric value",
			metadata: "npv0v001::cc=abc::nn=2::bn=0",
			// generic parse error, just check err != nil
		},
		{
			name:     "missing equals sign",
			metadata: "npv0v001::cc1::nn=2::bn=0",
			wantErr:  ErrMalformedMetadataPair,
		},
		{
			name:     "missing value after equals",
			metadata: "npv0v001::cc=::nn=2::bn=0",
			wantErr:  ErrMissingMetadataValue,
		},
		{
			name:     "missing key before equals",
			metadata: "npv0v001::nn=2::bn=0::=5",
			wantErr:  ErrMissingMetadataKey,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var pl Payload
			err := UnpackMetadataInto(&pl, tt.metadata)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestUnpackMetadataIntoInitializesFields(t *testing.T) {
	// Verify that fields are reset to unknownMetadataValue before parsing.
	// We use a valid metadata string, so all fields should be overwritten.
	pl := Payload{
		Containers:  999,
		NUMANodes:   999,
		BusiestNode: 999,
	}
	metadata := "npv0v001::cc=3::nn=2::bn=1"
	if err := UnpackMetadataInto(&pl, metadata); err != nil {
		t.Fatalf("UnpackMetadataInto() error = %v", err)
	}
	if pl.Containers != 3 {
		t.Errorf("Containers = %d, want 3", pl.Containers)
	}
	if pl.NUMANodes != 2 {
		t.Errorf("NUMANodes = %d, want 2", pl.NUMANodes)
	}
	if pl.BusiestNode != 1 {
		t.Errorf("BusiestNode = %d, want 1", pl.BusiestNode)
	}
}

func TestUnpackMetadataPartialUpdates(t *testing.T) {
	pl := Payload{
		Containers: 128,
	}
	metadata := "npv0v001::nn=8::bn=3"
	if err := UnpackMetadataInto(&pl, metadata); err != nil {
		t.Fatalf("UnpackMetadataInto() error = %v", err)
	}
	if pl.Containers != UnknownMetadataValue { // overridden to "unknwon"
		t.Errorf("Containers = %d, want -1", pl.Containers)
	}
	if pl.NUMANodes != 8 {
		t.Errorf("NUMANodes = %d, want 2", pl.NUMANodes)
	}
	if pl.BusiestNode != 3 {
		t.Errorf("BusiestNode = %d, want 1", pl.BusiestNode)
	}
}

func TestUnpackMetadataIntoVectorsUntouched(t *testing.T) {
	// Verify UnpackMetadataInto does not alter the Vectors field
	pl := Payload{
		Vectors: map[int]string{0: "test"},
	}
	metadata := "npv0v001::cc=1::nn=1::bn=0"
	if err := UnpackMetadataInto(&pl, metadata); err != nil {
		t.Fatalf("UnpackMetadataInto() error = %v", err)
	}
	if len(pl.Vectors) != 1 && pl.Vectors[0] != "test" {
		t.Errorf("Vectors was modified: got %v", pl.Vectors)
	}
}
