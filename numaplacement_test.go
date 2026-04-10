// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Red Hat, Inc.

package numaplacement

import (
	"errors"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestContainerIDString(t *testing.T) {
	tests := []struct {
		name     string
		cid      ContainerID
		expected string
	}{
		{
			name:     "empty",
			expected: "//",
		},
		{
			name: "filled",
			cid: ContainerID{
				Namespace:     "ns",
				PodName:       "pod",
				ContainerName: "cnt",
			},
			expected: "ns/pod/cnt",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cid.String()
			if got != tt.expected {
				t.Errorf("Mismatch: got=%q expected=%q", got, tt.expected)
			}
		})
	}
}

func TestContainerIDHash(t *testing.T) {
	tests := []struct {
		name     string
		cid      ContainerID
		expected uint64 // precomputed locally
	}{
		{
			name:     "empty",
			expected: 11145182160106660097,
		},
		{
			name: "filled",
			cid: ContainerID{
				Namespace:     "ns",
				PodName:       "pod",
				ContainerName: "cnt",
			},
			expected: 9468815510407277218,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cid.Hash()
			if got != tt.expected {
				t.Errorf("Mismatch: got=[%d] expected=[%d]", got, tt.expected)
			}
		})
	}
}

func TestInfoAffinity(t *testing.T) {
	runInfoAffinityTests(t, infoAffinityTestCases(), func(info *Info, cid ContainerID) (int, error) {
		return info.NUMAAffinity(cid)
	})
}

func TestInfoAffinityContainer(t *testing.T) {
	runInfoAffinityTests(t, infoAffinityTestCases(), func(info *Info, cid ContainerID) (int, error) {
		return info.NUMAAffinityContainer(cid.Namespace, cid.PodName, cid.ContainerName)
	})
}

type infoAffinityTestCase struct {
	name     string
	cid      ContainerID
	preFills []ContainerAffinity
	wantNode int
	wantErr  error
}

func infoAffinityTestCases() []infoAffinityTestCase {
	return []infoAffinityTestCase{
		{
			name:    "zero value container on empty info",
			wantErr: ErrUnknownContainer,
		},
		{
			name:    "empty info",
			cid:     ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "cnt"},
			wantErr: ErrUnknownContainer,
		},
		{
			name: "trivial match",
			preFills: []ContainerAffinity{
				{ID: ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "cnt"}, NUMANode: 2},
			},
			cid:      ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "cnt"},
			wantNode: 2,
		},
		{
			name: "match on NUMA 0",
			preFills: []ContainerAffinity{
				{ID: ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "cnt"}, NUMANode: 0},
			},
			cid:      ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "cnt"},
			wantNode: 0,
		},
		{
			name: "miss among multiple containers",
			preFills: []ContainerAffinity{
				{ID: ContainerID{Namespace: "ns", PodName: "pod-a", ContainerName: "cnt"}, NUMANode: 0},
				{ID: ContainerID{Namespace: "ns", PodName: "pod-b", ContainerName: "cnt"}, NUMANode: 1},
			},
			cid:     ContainerID{Namespace: "ns", PodName: "pod-c", ContainerName: "cnt"},
			wantErr: ErrUnknownContainer,
		},
		{
			name: "right namespace and pod wrong container",
			preFills: []ContainerAffinity{
				{ID: ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "app"}, NUMANode: 1},
			},
			cid:     ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "sidecar"},
			wantErr: ErrUnknownContainer,
		},
		{
			name: "multiple containers different NUMAs first",
			preFills: []ContainerAffinity{
				{ID: ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "app"}, NUMANode: 0},
				{ID: ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "sidecar"}, NUMANode: 3},
				{ID: ContainerID{Namespace: "other", PodName: "pod2", ContainerName: "worker"}, NUMANode: 7},
			},
			cid:      ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "app"},
			wantNode: 0,
		},
		{
			name: "multiple containers different NUMAs last",
			preFills: []ContainerAffinity{
				{ID: ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "app"}, NUMANode: 0},
				{ID: ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "sidecar"}, NUMANode: 3},
				{ID: ContainerID{Namespace: "other", PodName: "pod2", ContainerName: "worker"}, NUMANode: 7},
			},
			cid:      ContainerID{Namespace: "other", PodName: "pod2", ContainerName: "worker"},
			wantNode: 7,
		},
		{
			name: "different namespace same pod and container",
			preFills: []ContainerAffinity{
				{ID: ContainerID{Namespace: "ns-a", PodName: "pod", ContainerName: "cnt"}, NUMANode: 1},
				{ID: ContainerID{Namespace: "ns-b", PodName: "pod", ContainerName: "cnt"}, NUMANode: 5},
			},
			cid:      ContainerID{Namespace: "ns-b", PodName: "pod", ContainerName: "cnt"},
			wantNode: 5,
		},
		{
			name: "different pod same namespace and container",
			preFills: []ContainerAffinity{
				{ID: ContainerID{Namespace: "ns", PodName: "pod-a", ContainerName: "cnt"}, NUMANode: 2},
				{ID: ContainerID{Namespace: "ns", PodName: "pod-b", ContainerName: "cnt"}, NUMANode: 4},
			},
			cid:      ContainerID{Namespace: "ns", PodName: "pod-a", ContainerName: "cnt"},
			wantNode: 2,
		},
		{
			name: "different container same namespace and pod",
			preFills: []ContainerAffinity{
				{ID: ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "app"}, NUMANode: 0},
				{ID: ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "sidecar"}, NUMANode: 6},
			},
			cid:      ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "sidecar"},
			wantNode: 6,
		},
	}
}

func runInfoAffinityTests(t *testing.T, tests []infoAffinityTestCase, query func(info *Info, cid ContainerID) (int, error)) {
	t.Helper()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := NewInfo()
			for _, preFill := range tt.preFills {
				info.numaLocality[preFill.ID.Hash()] = preFill.NUMANode
			}
			gotNode, gotErr := query(info, tt.cid)
			if !errors.Is(gotErr, tt.wantErr) {
				t.Errorf("got error %v want %v", gotErr, tt.wantErr)
				return
			}
			if gotErr != nil {
				return
			}
			if gotNode != tt.wantNode {
				t.Errorf("node mismatch got=%d want=%d", gotNode, tt.wantNode)
			}
		})
	}
}

func TestInfoAffinityContainerMatchesNUMAAffinity(t *testing.T) {
	preFills := []ContainerAffinity{
		{ID: ContainerID{Namespace: "ns-a", PodName: "pod-1", ContainerName: "app"}, NUMANode: 0},
		{ID: ContainerID{Namespace: "ns-a", PodName: "pod-1", ContainerName: "sidecar"}, NUMANode: 3},
		{ID: ContainerID{Namespace: "ns-b", PodName: "pod-2", ContainerName: "worker"}, NUMANode: 7},
	}
	info := NewInfo()
	for _, pf := range preFills {
		info.numaLocality[pf.ID.Hash()] = pf.NUMANode
	}
	for _, pf := range preFills {
		nodeA, errA := info.NUMAAffinity(pf.ID)
		nodeB, errB := info.NUMAAffinityContainer(pf.ID.Namespace, pf.ID.PodName, pf.ID.ContainerName)
		if errA != errB {
			t.Errorf("%s: error mismatch NUMAAffinity=%v NUMAAffinityContainer=%v", pf.ID, errA, errB)
			continue
		}
		if nodeA != nodeB {
			t.Errorf("%s: node mismatch NUMAAffinity=%d NUMAAffinityContainer=%d", pf.ID, nodeA, nodeB)
		}
	}
}

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
		t.Errorf("NUMANodes = %d, want 8", pl.NUMANodes)
	}
	if pl.BusiestNode != 3 {
		t.Errorf("BusiestNode = %d, want 3", pl.BusiestNode)
	}
}

func TestPayloadValidate(t *testing.T) {
	tests := []struct {
		name    string
		pl      Payload
		wantErr error
	}{
		{
			name:    "empty",
			pl:      Payload{},
			wantErr: ErrInconsistentNUMANodes,
		},
		{
			name: "zero containers",
			pl: Payload{
				NUMANodes: 1, // needed to avoid ErrInconsistentNUMANodes
			},
			wantErr: nil,
		},
		{
			name: "negative containers",
			pl: Payload{
				Containers: -1,
				NUMANodes:  1, // needed to avoid ErrInconsistentNUMANodes
			},
			wantErr: ErrInconsistentContainerSet,
		},
		{
			name: "negative busiest node",
			pl: Payload{
				Containers:  1,
				NUMANodes:   1,
				BusiestNode: -1,
			},
			wantErr: ErrInconsistentBusiestNode,
		},
		{
			name: "oob busiest node",
			pl: Payload{
				Containers:  1,
				NUMANodes:   8,
				BusiestNode: 8,
			},
			wantErr: ErrInconsistentBusiestNode,
		},
		{
			name: "more vectors than containers",
			pl: Payload{
				NUMANodes: 1, // needed to avoid ErrInconsistentNUMANodes
				Vectors: map[int]string{
					0: "#$'H", // we don't even need a valid leb89 encoding - Validate will not check semantic correctness
				},
			},
			wantErr: ErrInconsistentNUMAVectors,
		},
		{
			name: "more vectors than NUMA nodes",
			pl: Payload{
				Containers: 8,
				NUMANodes:  1, // needed to avoid ErrInconsistentNUMANodes
				Vectors: map[int]string{
					0: "!#", // we don't even need a valid leb89 encoding - Validate will not check semantic correctness
					1: "$%",
				},
			},
			wantErr: ErrInconsistentNUMAVectors,
		},
		{
			name: "more vectors than NUMA nodes - highest ID",
			pl: Payload{
				Containers: 8,
				NUMANodes:  2,
				Vectors: map[int]string{
					2: "!#", // we don't even need a valid leb89 encoding - Validate will not check semantic correctness
				},
			},
			wantErr: ErrCorruptedNUMAVector,
		},
		{
			name: "vector with incorrect key",
			pl: Payload{
				Containers: 8,
				NUMANodes:  2,
				Vectors: map[int]string{
					-1: "!#", // we don't even need a valid leb89 encoding - Validate will not check semantic correctness
				},
			},
			wantErr: ErrCorruptedNUMAVector,
		},
		{
			name: "vector matching busiest node",
			pl: Payload{
				Containers:  8,
				NUMANodes:   2,
				BusiestNode: 1,
				Vectors: map[int]string{
					1: "$%'()*",
				},
			},
			wantErr: ErrCorruptedNUMAVector,
		},
		{
			name: "vector empty string is valid per spec",
			pl: Payload{
				Containers:  8,
				NUMANodes:   2,
				BusiestNode: 1,
				Vectors: map[int]string{
					0: "",
				},
			},
		},
		{
			name: "zero NUMA nodes",
			pl: Payload{
				Containers: 1,
				NUMANodes:  0,
			},
			wantErr: ErrInconsistentNUMANodes,
		},
		{
			name: "typical valid payload",
			pl: Payload{
				Containers:  4,
				NUMANodes:   2,
				BusiestNode: 0,
				Vectors: map[int]string{
					1: "!#",
				},
			},
		},
		{
			name: "valid payload multiple vectors",
			pl: Payload{
				Containers:  8,
				NUMANodes:   4,
				BusiestNode: 0,
				Vectors: map[int]string{
					1: "!#",
					2: "$%",
					3: "'(",
				},
			},
		},
		{
			name: "single NUMA all on busiest",
			pl: Payload{
				Containers:  5,
				NUMANodes:   1,
				BusiestNode: 0,
			},
		},
		{
			name: "busiest node at max valid",
			pl: Payload{
				Containers:  4,
				NUMANodes:   4,
				BusiestNode: 3,
				Vectors: map[int]string{
					0: "!#",
				},
			},
		},
		{
			name: "max valid vector count",
			pl: Payload{
				Containers:  8,
				NUMANodes:   3,
				BusiestNode: 0,
				Vectors: map[int]string{
					1: "!#",
					2: "$%",
				},
			},
		},
		{
			name: "zero containers nil vectors",
			pl: Payload{
				Containers: 0,
				NUMANodes:  2,
				Vectors:    nil,
			},
		},
		{
			name: "vector key at NUMANodes boundary",
			pl: Payload{
				Containers:  4,
				NUMANodes:   2,
				BusiestNode: 0,
				Vectors: map[int]string{
					2: "!#",
				},
			},
			wantErr: ErrCorruptedNUMAVector,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErr := tt.pl.Validate()
			if !errors.Is(gotErr, tt.wantErr) {
				t.Errorf("got error = %q, want = %q", gotErr, tt.wantErr)
			}
		})
	}

}

func TestInfoUpdate(t *testing.T) {
	ca1 := ContainerAffinity{
		ID:       ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
		NUMANode: 0,
	}
	ca2 := ContainerAffinity{
		ID:       ContainerID{Namespace: "ns2", PodName: "pod2", ContainerName: "cnt2"},
		NUMANode: 1,
	}
	ca3 := ContainerAffinity{
		ID:       ContainerID{Namespace: "ns3", PodName: "pod3", ContainerName: "cnt3"},
		NUMANode: 2,
	}

	t.Run("empty source into empty target", func(t *testing.T) {
		target := NewInfo()
		source := NewInfo()
		target.Update(source)

		_, err := target.NUMAAffinity(ca1.ID)
		if !errors.Is(err, ErrUnknownContainer) {
			t.Errorf("expected ErrUnknownContainer, got %v", err)
		}
	})

	t.Run("populated source into empty target", func(t *testing.T) {
		target := NewInfo()
		source := NewInfo()
		source.numaLocality[ca1.ID.Hash()] = ca1.NUMANode
		source.numaLocality[ca2.ID.Hash()] = ca2.NUMANode

		target.Update(source)

		for _, ca := range []ContainerAffinity{ca1, ca2} {
			got, err := target.NUMAAffinity(ca.ID)
			if err != nil {
				t.Errorf("unexpected error for %s: %v", ca.ID, err)
			}
			if got != ca.NUMANode {
				t.Errorf("node mismatch for %s: got=%d expected=%d", ca.ID, got, ca.NUMANode)
			}
		}
	})

	t.Run("overwrites existing target data", func(t *testing.T) {
		target := NewInfo()
		target.numaLocality[ca1.ID.Hash()] = ca1.NUMANode

		source := NewInfo()
		source.numaLocality[ca2.ID.Hash()] = ca2.NUMANode

		target.Update(source)

		// ca1 should no longer be in target
		_, err := target.NUMAAffinity(ca1.ID)
		if !errors.Is(err, ErrUnknownContainer) {
			t.Errorf("expected ErrUnknownContainer for replaced entry, got %v", err)
		}
		// ca2 should be in target
		got, err := target.NUMAAffinity(ca2.ID)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if got != ca2.NUMANode {
			t.Errorf("node mismatch: got=%d expected=%d", got, ca2.NUMANode)
		}
	})

	t.Run("clone independence from source", func(t *testing.T) {
		target := NewInfo()
		source := NewInfo()
		source.numaLocality[ca1.ID.Hash()] = ca1.NUMANode

		target.Update(source)

		// mutate source after Update
		source.numaLocality[ca3.ID.Hash()] = ca3.NUMANode
		source.numaLocality[ca1.ID.Hash()] = 99

		// target must be unaffected
		got, err := target.NUMAAffinity(ca1.ID)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if got != ca1.NUMANode {
			t.Errorf("target was mutated by source change: got=%d expected=%d", got, ca1.NUMANode)
		}
		_, err = target.NUMAAffinity(ca3.ID)
		if !errors.Is(err, ErrUnknownContainer) {
			t.Errorf("target should not see source additions, got %v", err)
		}
	})
}

func TestInfoTake(t *testing.T) {
	ca1 := ContainerAffinity{
		ID:       ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
		NUMANode: 0,
	}
	ca2 := ContainerAffinity{
		ID:       ContainerID{Namespace: "ns2", PodName: "pod2", ContainerName: "cnt2"},
		NUMANode: 1,
	}

	t.Run("empty source into empty target", func(t *testing.T) {
		target := NewInfo()
		source := NewInfo()
		target.Take(source)

		if source.numaLocality != nil {
			t.Error("source numaLocality should be nil after Take")
		}
		_, err := target.NUMAAffinity(ca1.ID)
		if !errors.Is(err, ErrUnknownContainer) {
			t.Errorf("expected ErrUnknownContainer, got %v", err)
		}
	})

	t.Run("populated source into empty target", func(t *testing.T) {
		target := NewInfo()
		source := NewInfo()
		source.numaLocality[ca1.ID.Hash()] = ca1.NUMANode
		source.numaLocality[ca2.ID.Hash()] = ca2.NUMANode

		target.Take(source)

		if source.numaLocality != nil {
			t.Error("source numaLocality should be nil after Take")
		}
		for _, ca := range []ContainerAffinity{ca1, ca2} {
			got, err := target.NUMAAffinity(ca.ID)
			if err != nil {
				t.Errorf("unexpected error for %s: %v", ca.ID, err)
			}
			if got != ca.NUMANode {
				t.Errorf("node mismatch for %s: got=%d expected=%d", ca.ID, got, ca.NUMANode)
			}
		}
	})

	t.Run("overwrites existing target data", func(t *testing.T) {
		target := NewInfo()
		target.numaLocality[ca1.ID.Hash()] = ca1.NUMANode

		source := NewInfo()
		source.numaLocality[ca2.ID.Hash()] = ca2.NUMANode

		target.Take(source)

		if source.numaLocality != nil {
			t.Error("source numaLocality should be nil after Take")
		}
		// ca1 should no longer be in target
		_, err := target.NUMAAffinity(ca1.ID)
		if !errors.Is(err, ErrUnknownContainer) {
			t.Errorf("expected ErrUnknownContainer for replaced entry, got %v", err)
		}
		// ca2 should be in target
		got, err := target.NUMAAffinity(ca2.ID)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if got != ca2.NUMANode {
			t.Errorf("node mismatch: got=%d expected=%d", got, ca2.NUMANode)
		}
	})

	t.Run("move semantics shares underlying map", func(t *testing.T) {
		target := NewInfo()
		source := NewInfo()
		source.numaLocality[ca1.ID.Hash()] = ca1.NUMANode

		target.Take(source)

		// mutate target map — this is expected since Take moves, not clones
		target.numaLocality[ca1.ID.Hash()] = 99

		got, err := target.NUMAAffinity(ca1.ID)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if got != 99 {
			t.Errorf("expected mutated value 99, got %d", got)
		}
	})
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
	if len(pl.Vectors) != 1 || pl.Vectors[0] != "test" {
		t.Errorf("Vectors was modified: got %v", pl.Vectors)
	}
}

func TestPerNUMAVectorRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		vec  []int32
		enc  string
		want []int32
	}{
		{
			name: "nil",
			vec:  nil,
			want: []int32{},
		},
		{
			name: "empty",
			vec:  []int32{},
			want: []int32{},
		},
		{
			name: "one element only",
			vec:  []int32{1},
			want: []int32{1},
		},
		{
			name: "small vector",
			vec:  []int32{1, 3, 7, 42},
			enc:  "#$'H",
			want: []int32{1, 3, 7, 42},
		},
		{
			name: "small vector at offset 0",
			vec:  []int32{0, 4, 8, 10},
			enc:  "!''$",
			want: []int32{0, 4, 8, 10},
		},
		{
			name: "2-char first entry",
			vec:  []int32{100, 108, 116},
			enc:  "fI++",
			want: []int32{100, 108, 116},
		},
		{
			name: "3-char first entry",
			vec:  []int32{2000, 2008},
			enc:  "gk3+",
			want: []int32{2000, 2008},
		},
		{
			name: "practical limit",
			vec:  []int32{1, 1 << 10},
			want: []int32{1, 1 << 10},
		},
		{
			name: "unrealistically dense",
			vec:  makeSliceUpTo(1 << 10),
			want: makeSliceUpTo(1 << 10),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vec := append([]int32{}, tt.vec...)
			slices.Sort(vec)
			tmp := EncodePerNUMAVector(vec)

			if tt.enc != "" {
				if tmp != tt.enc {
					t.Errorf("encoded string mismatch got=%q want=%q", tmp, tt.enc)
				}
			}

			want := append([]int32{}, tt.want...)
			slices.Sort(want)
			got := DecodePerNUMAVector(tmp)
			if diff := cmp.Diff(got, want); diff != "" {
				t.Errorf("roundtrip failure: %v", diff)
			}
		})
	}
}

func TestDecodePerNUMAVectorKnownStrings(t *testing.T) {
	tests := []struct {
		name string
		enc  string
		want []int32
	}{
		{
			name: "empty string",
			enc:  "",
			want: []int32{},
		},
		{
			name: "all-terminal deltas",
			enc:  "#$'H",
			want: []int32{1, 3, 7, 42},
		},
		{
			name: "starts at offset 0",
			enc:  "!''$",
			want: []int32{0, 4, 8, 10},
		},
		{
			name: "2-char first entry",
			enc:  "fI++",
			want: []int32{100, 108, 116},
		},
		{
			name: "3-char first entry",
			enc:  "gk3+",
			want: []int32{2000, 2008},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DecodePerNUMAVector(tt.enc)
			if diff := cmp.Diff(got, tt.want); diff != "" {
				t.Errorf("DecodePerNUMAVector(%q) = %v", tt.enc, diff)
			}
		})
	}
}

func TestNewEncoder(t *testing.T) {
	tests := []struct {
		name      string
		numaNodes int
		wantErr   error
	}{
		{
			name:      "negative NUMA Nodes",
			numaNodes: -1,
			wantErr:   ErrInconsistentNUMANodes,
		},
		{
			name:      "zero NUMA Nodes",
			numaNodes: 0,
			wantErr:   ErrInconsistentNUMANodes,
		},
		{
			name:      "1 NUMA Node",
			numaNodes: 1,
			wantErr:   nil,
		},
		{
			name:      "2 NUMA Node",
			numaNodes: 2,
			wantErr:   nil,
		},
		{
			name:      "8 NUMA Node",
			numaNodes: 8,
			wantErr:   nil,
		},
		{
			name:      "insane amount of NUMA Nodes",
			numaNodes: 65535,
			wantErr:   nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc, gotErr := NewEncoder(tt.numaNodes)
			if !errors.Is(gotErr, tt.wantErr) {
				t.Errorf("got error %v want %v", gotErr, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if enc.NUMANodes() != tt.numaNodes {
				t.Errorf("NUMA nodes mismatch got %d expected %d", enc.NUMANodes(), tt.numaNodes)
			}
		})
	}
}

func TestEncoderEncode(t *testing.T) {
	runEncoderTests(t, encoderTestCases(), func(enc *Encoder, ca ContainerAffinity) (*Encoder, error) {
		return enc.Encode(ca)
	})
}

func TestEncoderEncodeContainer(t *testing.T) {
	runEncoderTests(t, encoderTestCases(), func(enc *Encoder, ca ContainerAffinity) (*Encoder, error) {
		return enc.EncodeContainer(ca.ID.Namespace, ca.ID.PodName, ca.ID.ContainerName, ca.NUMANode)
	})
}

func TestEncoderEncodeContainerMatchesEncode(t *testing.T) {
	affinities := []ContainerAffinity{
		{ID: ContainerID{Namespace: "ns-a", PodName: "pod-1", ContainerName: "app"}, NUMANode: 0},
		{ID: ContainerID{Namespace: "ns-a", PodName: "pod-1", ContainerName: "sidecar"}, NUMANode: 1},
		{ID: ContainerID{Namespace: "ns-b", PodName: "pod-2", ContainerName: "worker"}, NUMANode: 0},
	}
	encA, err := NewEncoder(2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	encB, err := NewEncoder(2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, ca := range affinities {
		if _, err := encA.Encode(ca); err != nil {
			t.Fatalf("Encode(%s): %v", ca.ID, err)
		}
		if _, err := encB.EncodeContainer(ca.ID.Namespace, ca.ID.PodName, ca.ID.ContainerName, ca.NUMANode); err != nil {
			t.Fatalf("EncodeContainer(%s): %v", ca.ID, err)
		}
	}
	plA, errA := encA.Result()
	plB, errB := encB.Result()
	if errA != errB {
		t.Fatalf("error mismatch Encode=%v EncodeContainer=%v", errA, errB)
	}
	if diff := cmp.Diff(plA, plB); diff != "" {
		t.Errorf("Payload mismatch Encode vs EncodeContainer: %v", diff)
	}
}

func TestEncoderResult(t *testing.T) {
	tests := []struct {
		name      string
		numaNodes int
		affs      []ContainerAffinity
		wantPL    Payload
		wantErr   error
	}{
		{
			name:      "empty",
			numaNodes: 2,
			wantPL: Payload{
				NUMANodes:   2,
				BusiestNode: 0,
				Vectors:     map[int]string{},
			},
		},
		{
			name:      "single NUMA node, degenerate case",
			numaNodes: 1,
			affs: []ContainerAffinity{
				{ID: ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: ContainerID{Namespace: "ns1", PodName: "pod2", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: ContainerID{Namespace: "ns1", PodName: "pod3", ContainerName: "cnt1"}, NUMANode: 0},
			},
			wantPL: Payload{
				Containers:  3,
				NUMANodes:   1,
				BusiestNode: 0,
				Vectors:     map[int]string{},
			},
		},
		{
			name:      "all containers on one NUMA",
			numaNodes: 2,
			affs: []ContainerAffinity{
				{ID: ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: ContainerID{Namespace: "ns1", PodName: "pod2", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: ContainerID{Namespace: "ns1", PodName: "pod3", ContainerName: "cnt1"}, NUMANode: 0},
			},
			wantPL: Payload{
				Containers:  3,
				NUMANodes:   2,
				BusiestNode: 0,
				Vectors:     map[int]string{},
			},
		},
		{
			name:      "all containers on one NUMA, duplicate container",
			numaNodes: 2,
			affs: []ContainerAffinity{
				{ID: ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: ContainerID{Namespace: "ns1", PodName: "pod2", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: ContainerID{Namespace: "ns1", PodName: "pod2", ContainerName: "cnt1"}, NUMANode: 0},
			},
			wantPL: Payload{
				Containers:  2,
				NUMANodes:   2,
				BusiestNode: 0,
				Vectors:     map[int]string{},
			},
		},
		{
			name:      "uneven split",
			numaNodes: 2,
			affs: []ContainerAffinity{
				{ID: ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: ContainerID{Namespace: "ns1", PodName: "pod2", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: ContainerID{Namespace: "ns1", PodName: "pod3", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: ContainerID{Namespace: "ns2", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 1},
			},
			wantPL: Payload{
				Containers:  4,
				NUMANodes:   2,
				BusiestNode: 0,
				Vectors:     map[int]string{1: "#"},
			},
		},
		{
			name:      "even split tie-break lowest NUMA wins",
			numaNodes: 2,
			affs: []ContainerAffinity{
				{ID: ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: ContainerID{Namespace: "ns1", PodName: "pod2", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: ContainerID{Namespace: "ns2", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 1},
				{ID: ContainerID{Namespace: "ns2", PodName: "pod2", ContainerName: "cnt1"}, NUMANode: 1},
			},
			wantPL: Payload{
				Containers:  4,
				NUMANodes:   2,
				BusiestNode: 0,
				Vectors:     map[int]string{1: "!%"},
			},
		},
		{
			name:      "busiest is not NUMA 0",
			numaNodes: 2,
			affs: []ContainerAffinity{
				{ID: ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: ContainerID{Namespace: "ns2", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 1},
				{ID: ContainerID{Namespace: "ns2", PodName: "pod2", ContainerName: "cnt1"}, NUMANode: 1},
				{ID: ContainerID{Namespace: "ns2", PodName: "pod3", ContainerName: "cnt1"}, NUMANode: 1},
			},
			wantPL: Payload{
				Containers:  4,
				NUMANodes:   2,
				BusiestNode: 1,
				Vectors:     map[int]string{0: "$"},
			},
		},
		{
			name:      "3-way split across 4 NUMA Nodes",
			numaNodes: 4,
			affs: []ContainerAffinity{
				{ID: ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: ContainerID{Namespace: "ns1", PodName: "pod2", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: ContainerID{Namespace: "ns1", PodName: "pod3", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: ContainerID{Namespace: "ns2", PodName: "pod4", ContainerName: "cnt1"}, NUMANode: 1},
				{ID: ContainerID{Namespace: "ns3", PodName: "pod5", ContainerName: "cnt1"}, NUMANode: 2},
			},
			wantPL: Payload{
				Containers:  5,
				NUMANodes:   4,
				BusiestNode: 0,
				Vectors:     map[int]string{1: "!", 2: "'"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc, err := NewEncoder(tt.numaNodes)
			if err != nil {
				t.Fatalf("unexpected encoder create error: %v", err)
			}
			enc, err = enc.Encode(tt.affs...)
			if err != nil {
				t.Fatalf("unexpected encoding error: %v", err)
			}
			gotPL, err := enc.Result()
			if err != nil {
				t.Fatalf("unexpected encoder finalization error: %v", err)
			}
			if diff := cmp.Diff(gotPL, tt.wantPL); diff != "" {
				t.Errorf("unexpected payload content: %v", diff)
			}
		})
	}
}

func TestEncoderResultStreamingAccumulation(t *testing.T) {
	ca1 := ContainerAffinity{ID: ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 0}
	ca2 := ContainerAffinity{ID: ContainerID{Namespace: "ns1", PodName: "pod2", ContainerName: "cnt1"}, NUMANode: 0}
	ca3 := ContainerAffinity{ID: ContainerID{Namespace: "ns2", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 1}

	// Reference: all at once via constructor
	refEnc, err := NewEncoder(2, ca1, ca2, ca3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantPL, err := refEnc.Result()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tests := []struct {
		name   string
		encode func() (*Encoder, error)
	}{
		{
			name: "Encode then Encode",
			encode: func() (*Encoder, error) {
				enc, err := NewEncoder(2)
				if err != nil {
					return nil, err
				}
				if _, err := enc.Encode(ca1, ca2); err != nil {
					return nil, err
				}
				if _, err := enc.Encode(ca3); err != nil {
					return nil, err
				}
				return enc, nil
			},
		},
		{
			name: "EncodeContainer then EncodeContainer",
			encode: func() (*Encoder, error) {
				enc, err := NewEncoder(2)
				if err != nil {
					return nil, err
				}
				if _, err := enc.EncodeContainer(ca1.ID.Namespace, ca1.ID.PodName, ca1.ID.ContainerName, ca1.NUMANode); err != nil {
					return nil, err
				}
				if _, err := enc.EncodeContainer(ca2.ID.Namespace, ca2.ID.PodName, ca2.ID.ContainerName, ca2.NUMANode); err != nil {
					return nil, err
				}
				if _, err := enc.EncodeContainer(ca3.ID.Namespace, ca3.ID.PodName, ca3.ID.ContainerName, ca3.NUMANode); err != nil {
					return nil, err
				}
				return enc, nil
			},
		},
		{
			name: "constructor with two then Encode one",
			encode: func() (*Encoder, error) {
				enc, err := NewEncoder(2, ca1, ca2)
				if err != nil {
					return nil, err
				}
				if _, err := enc.Encode(ca3); err != nil {
					return nil, err
				}
				return enc, nil
			},
		},
		{
			name: "constructor with two then EncodeContainer one",
			encode: func() (*Encoder, error) {
				enc, err := NewEncoder(2, ca1, ca2)
				if err != nil {
					return nil, err
				}
				if _, err := enc.EncodeContainer(ca3.ID.Namespace, ca3.ID.PodName, ca3.ID.ContainerName, ca3.NUMANode); err != nil {
					return nil, err
				}
				return enc, nil
			},
		},
		{
			name: "Encode then EncodeContainer",
			encode: func() (*Encoder, error) {
				enc, err := NewEncoder(2)
				if err != nil {
					return nil, err
				}
				if _, err := enc.Encode(ca1); err != nil {
					return nil, err
				}
				if _, err := enc.EncodeContainer(ca2.ID.Namespace, ca2.ID.PodName, ca2.ID.ContainerName, ca2.NUMANode); err != nil {
					return nil, err
				}
				if _, err := enc.Encode(ca3); err != nil {
					return nil, err
				}
				return enc, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc, err := tt.encode()
			if err != nil {
				t.Fatalf("unexpected encoding error: %v", err)
			}
			gotPL, err := enc.Result()
			if err != nil {
				t.Fatalf("unexpected result error: %v", err)
			}
			if diff := cmp.Diff(gotPL, wantPL); diff != "" {
				t.Errorf("payload mismatch vs reference: %v", diff)
			}
		})
	}
}

func TestNewDecoder(t *testing.T) {
	tests := []struct {
		name     string
		payload  Payload
		preFills []ContainerID
		wantErr  error
	}{
		{
			name:    "invalid payload",
			payload: Payload{},
			wantErr: ErrInconsistentNUMANodes,
		},
		{
			name:    "valid empty payload",
			payload: EmptyPayload(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dec, gotErr := NewDecoder(tt.payload)
			if !errors.Is(gotErr, tt.wantErr) {
				t.Errorf("got error %v want %v", gotErr, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			gotCount := dec.Containers()
			if gotCount != len(tt.preFills) {
				t.Errorf("decoder size error got %d containers expected %d", gotCount, len(tt.preFills))
			}
		})
	}
}

func TestDecoderDecode(t *testing.T) {
	runDecoderTests(t, decoderTestCases(), func(dec *Decoder, ci ContainerID) *Decoder {
		return dec.Decode(ci)
	})
}

func TestDecoderDecodeContainer(t *testing.T) {
	runDecoderTests(t, decoderTestCases(), func(dec *Decoder, ci ContainerID) *Decoder {
		return dec.DecodeContainer(ci.Namespace, ci.PodName, ci.ContainerName)
	})
}

func TestDecoderDecodeContainerMatchesDecode(t *testing.T) {
	affinities := []ContainerAffinity{
		{ID: ContainerID{Namespace: "ns-a", PodName: "pod-1", ContainerName: "app"}, NUMANode: 0},
		{ID: ContainerID{Namespace: "ns-a", PodName: "pod-1", ContainerName: "sidecar"}, NUMANode: 1},
		{ID: ContainerID{Namespace: "ns-b", PodName: "pod-2", ContainerName: "worker"}, NUMANode: 0},
	}
	enc, err := NewEncoder(2, affinities...)
	if err != nil {
		t.Fatalf("unexpected encoder error: %v", err)
	}
	pl, err := enc.Result()
	if err != nil {
		t.Fatalf("unexpected result error: %v", err)
	}
	ids := make([]ContainerID, len(affinities))
	for i, ca := range affinities {
		ids[i] = ca.ID
	}
	decA, err := NewDecoder(pl)
	if err != nil {
		t.Fatalf("unexpected decoder create error: %v", err)
	}
	decB, err := NewDecoder(pl)
	if err != nil {
		t.Fatalf("unexpected decoder create error: %v", err)
	}
	for _, id := range ids {
		decA.Decode(id)
		decB.DecodeContainer(id.Namespace, id.PodName, id.ContainerName)
	}
	infoA, errA := decA.Result()
	infoB, errB := decB.Result()
	if errA != errB {
		t.Fatalf("error mismatch Decode=%v DecodeContainer=%v", errA, errB)
	}
	for _, ca := range affinities {
		nodeA, eA := infoA.NUMAAffinity(ca.ID)
		nodeB, eB := infoB.NUMAAffinity(ca.ID)
		if eA != eB {
			t.Errorf("%s: error mismatch Decode=%v DecodeContainer=%v", ca.ID, eA, eB)
			continue
		}
		if nodeA != nodeB {
			t.Errorf("%s: node mismatch Decode=%d DecodeContainer=%d", ca.ID, nodeA, nodeB)
		}
	}
}

func TestDecoderResult(t *testing.T) {
	tests := []struct {
		name     string
		payload  Payload
		idents   []ContainerID
		wantInfo *Info
		wantErr  error
	}{
		{
			name:     "empty",
			payload:  EmptyPayload(),
			wantInfo: NewInfo(),
		},
		{
			name:    "error path: inconsistent hashesSet and Containers - excess containers",
			payload: EmptyPayload(),
			idents: []ContainerID{
				{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
			},
			wantErr: ErrInconsistentContainerSet,
		},
		{
			name: "error path: inconsistent hashesSet and Containers - excess payload",
			payload: Payload{
				Containers: 2,
				NUMANodes:  2,
				Vectors: map[int]string{
					1: "!",
				},
			},
			wantErr: ErrInconsistentContainerSet,
		},
		{
			name: "error path: forged data: tampered vector encoding - inconsistent indexing",
			payload: Payload{
				Containers:  1,
				NUMANodes:   2,
				BusiestNode: 0,
				Vectors: map[int]string{
					1: "$",
				},
			},
			idents: []ContainerID{
				{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
			},
			wantErr: ErrCorruptedNUMAVector,
		},
		{
			name: "error path: forged data: tampered vector encoding - duplicate offset, same vector",
			payload: Payload{
				Containers:  5,
				NUMANodes:   2,
				BusiestNode: 0,
				Vectors: map[int]string{
					1: "#!",
				},
			},
			idents: []ContainerID{
				{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
				{Namespace: "ns1", PodName: "pod2", ContainerName: "cnt1"},
				{Namespace: "ns1", PodName: "pod3", ContainerName: "cnt1"},
				{Namespace: "ns2", PodName: "pod1", ContainerName: "cnt1"},
				{Namespace: "ns2", PodName: "pod2", ContainerName: "cnt1"},
			},
			wantErr: ErrDuplicatedNUMAVector,
		},
		{
			name: "error path: forged data: tampered vector encoding - duplicate offset, different vector",
			payload: Payload{
				Containers:  5,
				NUMANodes:   4,
				BusiestNode: 0,
				Vectors: map[int]string{
					1: "#",
					2: "#",
					3: "%",
				},
			},
			idents: []ContainerID{
				{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
				{Namespace: "ns2", PodName: "pod1", ContainerName: "cnt1"},
				{Namespace: "ns3", PodName: "pod1", ContainerName: "cnt1"},
				{Namespace: "ns4", PodName: "pod1", ContainerName: "cnt1"},
				{Namespace: "ns1", PodName: "pod2", ContainerName: "cnt1"},
			},
			wantErr: ErrDuplicatedNUMAVector,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dec, err := NewDecoder(tt.payload)
			if err != nil {
				t.Fatalf("unexpected decoder create error: %v", err)
			}
			_ = dec.Decode(tt.idents...)
			gotInfo, gotErr := dec.Result()
			if !errors.Is(gotErr, tt.wantErr) {
				t.Fatalf("got error %v expected %v", gotErr, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if gotInfo.Containers() != tt.wantInfo.Containers() {
				t.Errorf("got info not equal size to expected info")
			}
			if !gotInfo.Equal(tt.wantInfo) {
				t.Errorf("got info not equal to expected info")
			}
		})
	}
}

type encoderTestCase struct {
	name       string
	numaNodes  int
	affinities []ContainerAffinity
	wantCount  int
	wantErr    error
}

func encoderTestCases() []encoderTestCase {
	return []encoderTestCase{
		{
			name:      "no affinities",
			numaNodes: 1,
			wantCount: 0,
		},
		{
			name:      "trivial affinity",
			numaNodes: 1,
			affinities: []ContainerAffinity{
				{
					ID:       ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
					NUMANode: 0,
				},
			},
			wantCount: 1,
		},
		{
			name:      "unknown affinity",
			numaNodes: 1,
			affinities: []ContainerAffinity{
				{
					ID:       ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
					NUMANode: UnknownNUMAAffinity,
				},
			},
			wantErr: ErrUnsupportedUnknownNUMAAffinity,
		},
		{
			name:      "negative affinity",
			numaNodes: 1,
			affinities: []ContainerAffinity{
				{
					ID:       ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
					NUMANode: -3,
				},
			},
			wantErr: ErrWrongNUMAAffinity,
		},
		{
			name:      "edge case affinity",
			numaNodes: 4,
			affinities: []ContainerAffinity{
				{
					ID:       ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
					NUMANode: 3,
				},
			},
			wantCount: 1,
		},
		{
			name:      "OOB affinity",
			numaNodes: 4,
			affinities: []ContainerAffinity{
				{
					ID:       ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
					NUMANode: 15,
				},
			},
			wantErr: ErrWrongNUMAAffinity,
		},
		{
			name:      "simplest even distribution",
			numaNodes: 4,
			affinities: []ContainerAffinity{
				{ID: ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 2},
				{ID: ContainerID{Namespace: "ns1", PodName: "pod2", ContainerName: "cnt1"}, NUMANode: 1},
				{ID: ContainerID{Namespace: "ns2", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: ContainerID{Namespace: "ns2", PodName: "pod2", ContainerName: "cnt1"}, NUMANode: 3},
			},
			wantCount: 4,
		},
	}
}

func runEncoderTests(t *testing.T, tests []encoderTestCase, encode func(enc *Encoder, ca ContainerAffinity) (*Encoder, error)) {
	t.Helper()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc, gotErr := NewEncoder(tt.numaNodes)
			if gotErr != nil {
				t.Fatalf("unexpected error on creation: %v", gotErr)
			}
			for _, ca := range tt.affinities {
				_, gotErr = encode(enc, ca)
				if gotErr != nil {
					break
				}
			}
			if !errors.Is(gotErr, tt.wantErr) {
				t.Errorf("got error %v want %v", gotErr, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			gotCount := enc.Containers()
			if gotCount != tt.wantCount {
				t.Errorf("Container count mismatch got %d expected %d", gotCount, tt.wantCount)
			}
		})
	}
}

type decoderTestCase struct {
	name      string
	payload   Payload
	idents    []ContainerID
	wantCount int
}

func decoderTestCases() []decoderTestCase {
	return []decoderTestCase{
		{
			name:      "no idents",
			payload:   EmptyPayload(),
			wantCount: 0,
		},
		{
			name:    "trivial ident",
			payload: EmptyPayload(), // TODO
			idents: []ContainerID{
				{
					Namespace:     "ns1",
					PodName:       "pod1",
					ContainerName: "cnt1",
				},
			},
			wantCount: 1,
		},
		{
			name:    "duplicate ident",
			payload: EmptyPayload(), // TODO
			idents: []ContainerID{
				{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
				{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
			},
			wantCount: 1,
		},
	}
}

func runDecoderTests(t *testing.T, tests []decoderTestCase, decode func(dec *Decoder, ci ContainerID) *Decoder) {
	t.Helper()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dec, gotErr := NewDecoder(tt.payload)
			if gotErr != nil {
				t.Fatalf("unexpected error on creation: %v", gotErr)
			}
			for _, ci := range tt.idents {
				_ = decode(dec, ci)
			}
			gotCount := dec.Containers()
			if gotCount != tt.wantCount {
				t.Errorf("Container count mismatch got %d expected %d", gotCount, tt.wantCount)
			}
		})
	}
}

func makeSliceUpTo(lim int) []int32 {
	ret := []int32{}
	for idx := 0; idx < lim; idx++ {
		ret = append(ret, int32(idx))
	}
	return ret
}
