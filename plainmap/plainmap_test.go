// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Red Hat, Inc.

package plainmap

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/k8stopologyawareschedwg/numaplacement"
)

func TestInfoUpdate(t *testing.T) {
	ca1 := numaplacement.ContainerAffinity{
		ID:       numaplacement.ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
		NUMANode: 0,
	}
	ca2 := numaplacement.ContainerAffinity{
		ID:       numaplacement.ContainerID{Namespace: "ns2", PodName: "pod2", ContainerName: "cnt2"},
		NUMANode: 1,
	}
	ca3 := numaplacement.ContainerAffinity{
		ID:       numaplacement.ContainerID{Namespace: "ns3", PodName: "pod3", ContainerName: "cnt3"},
		NUMANode: 2,
	}

	t.Run("empty source into empty target", func(t *testing.T) {
		target := NewPlainInfo()
		source := NewPlainInfo()
		target.Update(source)

		_, err := target.NUMAAffinity(ca1.ID)
		if !errors.Is(err, numaplacement.ErrUnknownContainer) {
			t.Errorf("expected ErrUnknownContainer, got %v", err)
		}
	})

	t.Run("populated source into empty target", func(t *testing.T) {
		target := NewPlainInfo()
		source := NewPlainInfo()
		source.numaLocality[ca1.ID.String()] = ca1.NUMANode
		source.numaLocality[ca2.ID.String()] = ca2.NUMANode

		target.Update(source)

		for _, ca := range []numaplacement.ContainerAffinity{ca1, ca2} {
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
		target := NewPlainInfo()
		target.numaLocality[ca1.ID.String()] = ca1.NUMANode

		source := NewPlainInfo()
		source.numaLocality[ca2.ID.String()] = ca2.NUMANode

		target.Update(source)

		// ca1 should no longer be in target
		_, err := target.NUMAAffinity(ca1.ID)
		if !errors.Is(err, numaplacement.ErrUnknownContainer) {
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
		target := NewPlainInfo()
		source := NewPlainInfo()
		source.numaLocality[ca1.ID.String()] = ca1.NUMANode

		target.Update(source)

		// mutate source after Update
		source.numaLocality[ca3.ID.String()] = ca3.NUMANode
		source.numaLocality[ca1.ID.String()] = 99

		// target must be unaffected
		got, err := target.NUMAAffinity(ca1.ID)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if got != ca1.NUMANode {
			t.Errorf("target was mutated by source change: got=%d expected=%d", got, ca1.NUMANode)
		}
		_, err = target.NUMAAffinity(ca3.ID)
		if !errors.Is(err, numaplacement.ErrUnknownContainer) {
			t.Errorf("target should not see source additions, got %v", err)
		}
	})
}

func TestInfoTake(t *testing.T) {
	ca1 := numaplacement.ContainerAffinity{
		ID:       numaplacement.ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
		NUMANode: 0,
	}
	ca2 := numaplacement.ContainerAffinity{
		ID:       numaplacement.ContainerID{Namespace: "ns2", PodName: "pod2", ContainerName: "cnt2"},
		NUMANode: 1,
	}

	t.Run("empty source into empty target", func(t *testing.T) {
		target := NewPlainInfo()
		source := NewPlainInfo()
		target.Take(source)

		if source.numaLocality != nil {
			t.Error("source numaLocality should be nil after Take")
		}
		_, err := target.NUMAAffinity(ca1.ID)
		if !errors.Is(err, numaplacement.ErrUnknownContainer) {
			t.Errorf("expected ErrUnknownContainer, got %v", err)
		}
	})

	t.Run("populated source into empty target", func(t *testing.T) {
		target := NewPlainInfo()
		source := NewPlainInfo()
		source.numaLocality[ca1.ID.String()] = ca1.NUMANode
		source.numaLocality[ca2.ID.String()] = ca2.NUMANode

		target.Take(source)

		if source.numaLocality != nil {
			t.Error("source numaLocality should be nil after Take")
		}
		for _, ca := range []numaplacement.ContainerAffinity{ca1, ca2} {
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
		target := NewPlainInfo()
		target.numaLocality[ca1.ID.String()] = ca1.NUMANode

		source := NewPlainInfo()
		source.numaLocality[ca2.ID.String()] = ca2.NUMANode

		target.Take(source)

		if source.numaLocality != nil {
			t.Error("source numaLocality should be nil after Take")
		}
		// ca1 should no longer be in target
		_, err := target.NUMAAffinity(ca1.ID)
		if !errors.Is(err, numaplacement.ErrUnknownContainer) {
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
		target := NewPlainInfo()
		source := NewPlainInfo()
		source.numaLocality[ca1.ID.String()] = ca1.NUMANode

		target.Take(source)

		// mutate target map — this is expected since Take moves, not clones
		target.numaLocality[ca1.ID.String()] = 99

		got, err := target.NUMAAffinity(ca1.ID)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if got != 99 {
			t.Errorf("expected mutated value 99, got %d", got)
		}
	})
}

func TestInfoAffinity(t *testing.T) {
	runInfoAffinityTests(t, infoAffinityTestCases(), func(info numaplacement.Info, cid numaplacement.ContainerID) (int, error) {
		return info.NUMAAffinity(cid)
	})
}

func TestInfoAffinityContainer(t *testing.T) {
	runInfoAffinityTests(t, infoAffinityTestCases(), func(info numaplacement.Info, cid numaplacement.ContainerID) (int, error) {
		return info.NUMAAffinityContainer(cid.Namespace, cid.PodName, cid.ContainerName)
	})
}

type infoAffinityTestCase struct {
	name     string
	cid      numaplacement.ContainerID
	preFills []numaplacement.ContainerAffinity
	wantNode int
	wantErr  error
}

func infoAffinityTestCases() []infoAffinityTestCase {
	return []infoAffinityTestCase{
		{
			name:    "zero value container on empty info",
			wantErr: numaplacement.ErrUnknownContainer,
		},
		{
			name:    "empty info",
			cid:     numaplacement.ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "cnt"},
			wantErr: numaplacement.ErrUnknownContainer,
		},
		{
			name: "trivial match",
			preFills: []numaplacement.ContainerAffinity{
				{ID: numaplacement.ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "cnt"}, NUMANode: 2},
			},
			cid:      numaplacement.ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "cnt"},
			wantNode: 2,
		},
		{
			name: "match on NUMA 0",
			preFills: []numaplacement.ContainerAffinity{
				{ID: numaplacement.ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "cnt"}, NUMANode: 0},
			},
			cid:      numaplacement.ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "cnt"},
			wantNode: 0,
		},
		{
			name: "miss among multiple containers",
			preFills: []numaplacement.ContainerAffinity{
				{ID: numaplacement.ContainerID{Namespace: "ns", PodName: "pod-a", ContainerName: "cnt"}, NUMANode: 0},
				{ID: numaplacement.ContainerID{Namespace: "ns", PodName: "pod-b", ContainerName: "cnt"}, NUMANode: 1},
			},
			cid:     numaplacement.ContainerID{Namespace: "ns", PodName: "pod-c", ContainerName: "cnt"},
			wantErr: numaplacement.ErrUnknownContainer,
		},
		{
			name: "right namespace and pod wrong container",
			preFills: []numaplacement.ContainerAffinity{
				{ID: numaplacement.ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "app"}, NUMANode: 1},
			},
			cid:     numaplacement.ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "sidecar"},
			wantErr: numaplacement.ErrUnknownContainer,
		},
		{
			name: "multiple containers different NUMAs first",
			preFills: []numaplacement.ContainerAffinity{
				{ID: numaplacement.ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "app"}, NUMANode: 0},
				{ID: numaplacement.ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "sidecar"}, NUMANode: 3},
				{ID: numaplacement.ContainerID{Namespace: "other", PodName: "pod2", ContainerName: "worker"}, NUMANode: 7},
			},
			cid:      numaplacement.ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "app"},
			wantNode: 0,
		},
		{
			name: "multiple containers different NUMAs last",
			preFills: []numaplacement.ContainerAffinity{
				{ID: numaplacement.ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "app"}, NUMANode: 0},
				{ID: numaplacement.ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "sidecar"}, NUMANode: 3},
				{ID: numaplacement.ContainerID{Namespace: "other", PodName: "pod2", ContainerName: "worker"}, NUMANode: 7},
			},
			cid:      numaplacement.ContainerID{Namespace: "other", PodName: "pod2", ContainerName: "worker"},
			wantNode: 7,
		},
		{
			name: "different namespace same pod and container",
			preFills: []numaplacement.ContainerAffinity{
				{ID: numaplacement.ContainerID{Namespace: "ns-a", PodName: "pod", ContainerName: "cnt"}, NUMANode: 1},
				{ID: numaplacement.ContainerID{Namespace: "ns-b", PodName: "pod", ContainerName: "cnt"}, NUMANode: 5},
			},
			cid:      numaplacement.ContainerID{Namespace: "ns-b", PodName: "pod", ContainerName: "cnt"},
			wantNode: 5,
		},
		{
			name: "different pod same namespace and container",
			preFills: []numaplacement.ContainerAffinity{
				{ID: numaplacement.ContainerID{Namespace: "ns", PodName: "pod-a", ContainerName: "cnt"}, NUMANode: 2},
				{ID: numaplacement.ContainerID{Namespace: "ns", PodName: "pod-b", ContainerName: "cnt"}, NUMANode: 4},
			},
			cid:      numaplacement.ContainerID{Namespace: "ns", PodName: "pod-a", ContainerName: "cnt"},
			wantNode: 2,
		},
		{
			name: "different container same namespace and pod",
			preFills: []numaplacement.ContainerAffinity{
				{ID: numaplacement.ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "app"}, NUMANode: 0},
				{ID: numaplacement.ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "sidecar"}, NUMANode: 6},
			},
			cid:      numaplacement.ContainerID{Namespace: "ns", PodName: "pod", ContainerName: "sidecar"},
			wantNode: 6,
		},
	}
}

func runInfoAffinityTests(t *testing.T, tests []infoAffinityTestCase, query func(info numaplacement.Info, cid numaplacement.ContainerID) (int, error)) {
	t.Helper()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := NewPlainInfo()
			for _, preFill := range tt.preFills {
				info.numaLocality[preFill.ID.String()] = preFill.NUMANode
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
	preFills := []numaplacement.ContainerAffinity{
		{ID: numaplacement.ContainerID{Namespace: "ns-a", PodName: "pod-1", ContainerName: "app"}, NUMANode: 0},
		{ID: numaplacement.ContainerID{Namespace: "ns-a", PodName: "pod-1", ContainerName: "sidecar"}, NUMANode: 3},
		{ID: numaplacement.ContainerID{Namespace: "ns-b", PodName: "pod-2", ContainerName: "worker"}, NUMANode: 7},
	}
	info := NewPlainInfo()
	for _, pf := range preFills {
		info.numaLocality[pf.ID.String()] = pf.NUMANode
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

func TestNewEncoder(t *testing.T) {
	tests := []struct {
		name      string
		numaNodes int
		wantErr   error
	}{
		{
			name:      "negative NUMA Nodes",
			numaNodes: -1,
			wantErr:   numaplacement.ErrInconsistentNUMANodes,
		},
		{
			name:      "zero NUMA Nodes",
			numaNodes: 0,
			wantErr:   numaplacement.ErrInconsistentNUMANodes,
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
	runEncoderTests(t, encoderTestCases(), func(enc *Encoder, ca numaplacement.ContainerAffinity) (*Encoder, error) {
		return enc.Encode(ca)
	})
}

func TestEncoderEncodeContainer(t *testing.T) {
	runEncoderTests(t, encoderTestCases(), func(enc *Encoder, ca numaplacement.ContainerAffinity) (*Encoder, error) {
		return enc.EncodeContainer(ca.ID.Namespace, ca.ID.PodName, ca.ID.ContainerName, ca.NUMANode)
	})
}

func TestEncoderResult(t *testing.T) {
	tests := []struct {
		name      string
		numaNodes int
		affs      []numaplacement.ContainerAffinity
		wantPL    numaplacement.Payload
		wantErr   error
	}{
		{
			name:      "empty",
			numaNodes: 2,
			wantPL: numaplacement.Payload{
				NUMANodes:      2,
				BusiestNode:    0,
				VectorEncoding: VectorEncodingPlain,
				Vectors:        map[int]string{},
			},
		},
		{
			name:      "single NUMA node, degenerate case",
			numaNodes: 1,
			affs: []numaplacement.ContainerAffinity{
				{ID: numaplacement.ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: numaplacement.ContainerID{Namespace: "ns1", PodName: "pod2", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: numaplacement.ContainerID{Namespace: "ns1", PodName: "pod3", ContainerName: "cnt1"}, NUMANode: 0},
			},
			wantPL: numaplacement.Payload{
				Containers:     3,
				NUMANodes:      1,
				BusiestNode:    0,
				VectorEncoding: VectorEncodingPlain,
				Vectors:        map[int]string{0: "ns1/pod1/cnt1|ns1/pod2/cnt1|ns1/pod3/cnt1"},
			},
		},
		{
			name:      "all containers on one NUMA",
			numaNodes: 2,
			affs: []numaplacement.ContainerAffinity{
				{ID: numaplacement.ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: numaplacement.ContainerID{Namespace: "ns1", PodName: "pod2", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: numaplacement.ContainerID{Namespace: "ns1", PodName: "pod3", ContainerName: "cnt1"}, NUMANode: 0},
			},
			wantPL: numaplacement.Payload{
				Containers:     3,
				NUMANodes:      2,
				BusiestNode:    0,
				VectorEncoding: VectorEncodingPlain,
				Vectors:        map[int]string{0: "ns1/pod1/cnt1|ns1/pod2/cnt1|ns1/pod3/cnt1"},
			},
		},
		{
			name:      "all containers on one NUMA, duplicate container",
			numaNodes: 2,
			affs: []numaplacement.ContainerAffinity{
				{ID: numaplacement.ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: numaplacement.ContainerID{Namespace: "ns1", PodName: "pod2", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: numaplacement.ContainerID{Namespace: "ns1", PodName: "pod2", ContainerName: "cnt1"}, NUMANode: 0},
			},
			wantPL: numaplacement.Payload{
				Containers:     2,
				NUMANodes:      2,
				BusiestNode:    0,
				VectorEncoding: VectorEncodingPlain,
				Vectors:        map[int]string{0: "ns1/pod1/cnt1|ns1/pod2/cnt1"},
			},
		},
		{
			name:      "uneven split",
			numaNodes: 2,
			affs: []numaplacement.ContainerAffinity{
				{ID: numaplacement.ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: numaplacement.ContainerID{Namespace: "ns1", PodName: "pod2", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: numaplacement.ContainerID{Namespace: "ns1", PodName: "pod3", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: numaplacement.ContainerID{Namespace: "ns2", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 1},
			},
			wantPL: numaplacement.Payload{
				Containers:     4,
				NUMANodes:      2,
				BusiestNode:    0,
				VectorEncoding: VectorEncodingPlain,
				Vectors:        map[int]string{0: "ns1/pod1/cnt1|ns1/pod2/cnt1|ns1/pod3/cnt1", 1: "ns2/pod1/cnt1"},
			},
		},
		{
			name:      "even split",
			numaNodes: 2,
			affs: []numaplacement.ContainerAffinity{
				{ID: numaplacement.ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: numaplacement.ContainerID{Namespace: "ns1", PodName: "pod2", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: numaplacement.ContainerID{Namespace: "ns2", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 1},
				{ID: numaplacement.ContainerID{Namespace: "ns2", PodName: "pod2", ContainerName: "cnt1"}, NUMANode: 1},
			},
			wantPL: numaplacement.Payload{
				Containers:     4,
				NUMANodes:      2,
				BusiestNode:    0,
				VectorEncoding: VectorEncodingPlain,
				Vectors:        map[int]string{0: "ns1/pod1/cnt1|ns1/pod2/cnt1", 1: "ns2/pod1/cnt1|ns2/pod2/cnt1"},
			},
		},
		{
			name:      "more containers on NUMA 1",
			numaNodes: 2,
			affs: []numaplacement.ContainerAffinity{
				{ID: numaplacement.ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: numaplacement.ContainerID{Namespace: "ns2", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 1},
				{ID: numaplacement.ContainerID{Namespace: "ns2", PodName: "pod2", ContainerName: "cnt1"}, NUMANode: 1},
				{ID: numaplacement.ContainerID{Namespace: "ns2", PodName: "pod3", ContainerName: "cnt1"}, NUMANode: 1},
			},
			wantPL: numaplacement.Payload{
				Containers:     4,
				NUMANodes:      2,
				BusiestNode:    0,
				VectorEncoding: VectorEncodingPlain,
				Vectors:        map[int]string{0: "ns1/pod1/cnt1", 1: "ns2/pod1/cnt1|ns2/pod2/cnt1|ns2/pod3/cnt1"},
			},
		},
		{
			name:      "3-way split across 4 NUMA Nodes",
			numaNodes: 4,
			affs: []numaplacement.ContainerAffinity{
				{ID: numaplacement.ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: numaplacement.ContainerID{Namespace: "ns1", PodName: "pod2", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: numaplacement.ContainerID{Namespace: "ns1", PodName: "pod3", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: numaplacement.ContainerID{Namespace: "ns2", PodName: "pod4", ContainerName: "cnt1"}, NUMANode: 1},
				{ID: numaplacement.ContainerID{Namespace: "ns3", PodName: "pod5", ContainerName: "cnt1"}, NUMANode: 2},
			},
			wantPL: numaplacement.Payload{
				Containers:     5,
				NUMANodes:      4,
				BusiestNode:    0,
				VectorEncoding: VectorEncodingPlain,
				Vectors:        map[int]string{0: "ns1/pod1/cnt1|ns1/pod2/cnt1|ns1/pod3/cnt1", 1: "ns2/pod4/cnt1", 2: "ns3/pod5/cnt1"},
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
	ca1 := numaplacement.ContainerAffinity{ID: numaplacement.ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 0}
	ca2 := numaplacement.ContainerAffinity{ID: numaplacement.ContainerID{Namespace: "ns1", PodName: "pod2", ContainerName: "cnt1"}, NUMANode: 0}
	ca3 := numaplacement.ContainerAffinity{ID: numaplacement.ContainerID{Namespace: "ns2", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 1}

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
		payload  numaplacement.Payload
		preFills []numaplacement.ContainerID
		wantErr  error
	}{
		{
			name:    "invalid payload",
			payload: numaplacement.EmptyPayload(numaplacement.VectorEncodingLEB89),
			wantErr: numaplacement.ErrUnsupportedVectorEncoding,
		},
		{
			name:    "valid empty payload",
			payload: numaplacement.EmptyPayload(VectorEncodingPlain),
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
	runDecoderTests(t, decoderTestCases(), func(dec *Decoder, ci numaplacement.ContainerID) *Decoder {
		return dec.Decode(ci)
	})
}

func TestDecoderDecodeContainer(t *testing.T) {
	runDecoderTests(t, decoderTestCases(), func(dec *Decoder, ci numaplacement.ContainerID) *Decoder {
		return dec.DecodeContainer(ci.Namespace, ci.PodName, ci.ContainerName)
	})
}

func TestDecoderDecodeContainerMatchesDecode(t *testing.T) {
	affinities := []numaplacement.ContainerAffinity{
		{ID: numaplacement.ContainerID{Namespace: "ns-a", PodName: "pod-1", ContainerName: "app"}, NUMANode: 0},
		{ID: numaplacement.ContainerID{Namespace: "ns-a", PodName: "pod-1", ContainerName: "sidecar"}, NUMANode: 1},
		{ID: numaplacement.ContainerID{Namespace: "ns-b", PodName: "pod-2", ContainerName: "worker"}, NUMANode: 0},
	}
	enc, err := NewEncoder(2, affinities...)
	if err != nil {
		t.Fatalf("unexpected encoder error: %v", err)
	}
	pl, err := enc.Result()
	if err != nil {
		t.Fatalf("unexpected result error: %v", err)
	}
	ids := make([]numaplacement.ContainerID, len(affinities))
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
		payload  numaplacement.Payload
		idents   []numaplacement.ContainerID
		wantInfo *PlainInfo
		wantErr  error
	}{
		{
			name:     "empty",
			payload:  numaplacement.EmptyPayload(VectorEncodingPlain),
			wantInfo: NewPlainInfo(),
		},
		{
			name:    "error path: inconsistent hashesSet and Containers - excess containers",
			payload: numaplacement.EmptyPayload(VectorEncodingPlain),
			idents: []numaplacement.ContainerID{
				{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
			},
			wantErr: numaplacement.ErrInconsistentContainerSet,
		},
		{
			name: "error path: inconsistent hashesSet and Containers - excess payload",
			payload: numaplacement.Payload{
				Containers:     2,
				NUMANodes:      2,
				VectorEncoding: VectorEncodingPlain,
				Vectors: map[int]string{
					1: "!",
				},
			},
			wantErr: numaplacement.ErrInconsistentContainerSet,
		},
		{
			name: "error path: forged data: tampered vector encoding - inconsistent indexing",
			payload: numaplacement.Payload{
				Containers:     1,
				NUMANodes:      2,
				BusiestNode:    0,
				VectorEncoding: VectorEncodingPlain,
				Vectors: map[int]string{
					1: "$",
				},
			},
			idents: []numaplacement.ContainerID{
				{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
			},
			wantErr: numaplacement.ErrCorruptedNUMAVector,
		},
		{
			name: "error path: forged data: duplicate triple, same vector",
			payload: numaplacement.Payload{
				Containers:     3,
				NUMANodes:      2,
				BusiestNode:    0,
				VectorEncoding: VectorEncodingPlain,
				Vectors: map[int]string{
					1: "ns1/pod1/cnt1|ns1/pod1/cnt1",
				},
			},
			idents: []numaplacement.ContainerID{
				{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
				{Namespace: "ns1", PodName: "pod2", ContainerName: "cnt1"},
				{Namespace: "ns2", PodName: "pod1", ContainerName: "cnt1"},
			},
			wantErr: numaplacement.ErrDuplicatedNUMAVector,
		},
		{
			name: "error path: forged data: duplicate triple, different vector",
			payload: numaplacement.Payload{
				Containers:     3,
				NUMANodes:      4,
				BusiestNode:    0,
				VectorEncoding: VectorEncodingPlain,
				Vectors: map[int]string{
					1: "ns1/pod1/cnt1",
					2: "ns1/pod1/cnt1",
				},
			},
			idents: []numaplacement.ContainerID{
				{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
				{Namespace: "ns1", PodName: "pod2", ContainerName: "cnt1"},
				{Namespace: "ns2", PodName: "pod1", ContainerName: "cnt1"},
			},
			wantErr: numaplacement.ErrDuplicatedNUMAVector,
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
			encInfo, ok := gotInfo.(*PlainInfo)
			if !ok {
				t.Fatalf("unexpected Info concrete type %T", gotInfo)
			}
			if !encInfo.Equal(tt.wantInfo) {
				t.Errorf("got info not equal to expected info")
			}
		})
	}
}

func TestSeparatorSafetyZeroValuedContainerID(t *testing.T) {
	// Zero-valued ContainerID produces "//" via String().
	// Verify roundtrip doesn't confuse the "|" separator.
	affinities := []numaplacement.ContainerAffinity{
		{ID: numaplacement.ContainerID{}, NUMANode: 0},
		{ID: numaplacement.ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 1},
	}
	enc, err := NewEncoder(2, affinities...)
	if err != nil {
		t.Fatalf("unexpected encoder error: %v", err)
	}
	pl, err := enc.Result()
	if err != nil {
		t.Fatalf("unexpected result error: %v", err)
	}

	dec, err := NewDecoder(pl)
	if err != nil {
		t.Fatalf("unexpected decoder create error: %v", err)
	}
	for _, ca := range affinities {
		dec.Decode(ca.ID)
	}
	info, err := dec.Result()
	if err != nil {
		t.Fatalf("unexpected decoder result error: %v", err)
	}
	for _, ca := range affinities {
		got, err := info.NUMAAffinity(ca.ID)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", ca.ID, err)
			continue
		}
		if got != ca.NUMANode {
			t.Errorf("%s: node mismatch got=%d want=%d", ca.ID, got, ca.NUMANode)
		}
	}
}

func TestDecoderResultMalformedVectorStrings(t *testing.T) {
	tests := []struct {
		name    string
		payload numaplacement.Payload
		idents  []numaplacement.ContainerID
		wantErr error
	}{
		{
			name: "leading separator",
			payload: numaplacement.Payload{
				Containers:     1,
				NUMANodes:      2,
				VectorEncoding: VectorEncodingPlain,
				Vectors: map[int]string{
					0: "|ns1/pod1/cnt1",
				},
			},
			idents: []numaplacement.ContainerID{
				{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
			},
			wantErr: numaplacement.ErrCorruptedNUMAVector,
		},
		{
			name: "trailing separator",
			payload: numaplacement.Payload{
				Containers:     1,
				NUMANodes:      2,
				VectorEncoding: VectorEncodingPlain,
				Vectors: map[int]string{
					0: "ns1/pod1/cnt1|",
				},
			},
			idents: []numaplacement.ContainerID{
				{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
			},
			wantErr: numaplacement.ErrCorruptedNUMAVector,
		},
		{
			name: "double separator",
			payload: numaplacement.Payload{
				Containers:     2,
				NUMANodes:      2,
				VectorEncoding: VectorEncodingPlain,
				Vectors: map[int]string{
					0: "ns1/pod1/cnt1||ns1/pod2/cnt1",
				},
			},
			idents: []numaplacement.ContainerID{
				{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
				{Namespace: "ns1", PodName: "pod2", ContainerName: "cnt1"},
			},
			wantErr: numaplacement.ErrCorruptedNUMAVector,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dec, err := NewDecoder(tt.payload)
			if err != nil {
				t.Fatalf("unexpected decoder create error: %v", err)
			}
			_ = dec.Decode(tt.idents...)
			_, gotErr := dec.Result()
			if !errors.Is(gotErr, tt.wantErr) {
				t.Fatalf("got error %v expected %v", gotErr, tt.wantErr)
			}
		})
	}
}

type encoderTestCase struct {
	name       string
	numaNodes  int
	affinities []numaplacement.ContainerAffinity
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
			affinities: []numaplacement.ContainerAffinity{
				{
					ID:       numaplacement.ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
					NUMANode: 0,
				},
			},
			wantCount: 1,
		},
		{
			name:      "unknown affinity",
			numaNodes: 1,
			affinities: []numaplacement.ContainerAffinity{
				{
					ID:       numaplacement.ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
					NUMANode: numaplacement.UnknownNUMAAffinity,
				},
			},
			wantErr: numaplacement.ErrUnsupportedUnknownNUMAAffinity,
		},
		{
			name:      "negative affinity",
			numaNodes: 1,
			affinities: []numaplacement.ContainerAffinity{
				{
					ID:       numaplacement.ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
					NUMANode: -3,
				},
			},
			wantErr: numaplacement.ErrWrongNUMAAffinity,
		},
		{
			name:      "edge case affinity",
			numaNodes: 4,
			affinities: []numaplacement.ContainerAffinity{
				{
					ID:       numaplacement.ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
					NUMANode: 3,
				},
			},
			wantCount: 1,
		},
		{
			name:      "OOB affinity",
			numaNodes: 4,
			affinities: []numaplacement.ContainerAffinity{
				{
					ID:       numaplacement.ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
					NUMANode: 15,
				},
			},
			wantErr: numaplacement.ErrWrongNUMAAffinity,
		},
		{
			name:      "simplest even distribution",
			numaNodes: 4,
			affinities: []numaplacement.ContainerAffinity{
				{ID: numaplacement.ContainerID{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 2},
				{ID: numaplacement.ContainerID{Namespace: "ns1", PodName: "pod2", ContainerName: "cnt1"}, NUMANode: 1},
				{ID: numaplacement.ContainerID{Namespace: "ns2", PodName: "pod1", ContainerName: "cnt1"}, NUMANode: 0},
				{ID: numaplacement.ContainerID{Namespace: "ns2", PodName: "pod2", ContainerName: "cnt1"}, NUMANode: 3},
			},
			wantCount: 4,
		},
	}
}

func runEncoderTests(t *testing.T, tests []encoderTestCase, encode func(enc *Encoder, ca numaplacement.ContainerAffinity) (*Encoder, error)) {
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
	payload   numaplacement.Payload
	idents    []numaplacement.ContainerID
	wantCount int
}

func decoderTestCases() []decoderTestCase {
	return []decoderTestCase{
		{
			name:      "no idents",
			payload:   numaplacement.EmptyPayload(VectorEncodingPlain),
			wantCount: 0,
		},
		{
			name:    "trivial ident",
			payload: numaplacement.EmptyPayload(VectorEncodingPlain),
			idents: []numaplacement.ContainerID{
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
			payload: numaplacement.EmptyPayload(VectorEncodingPlain),
			idents: []numaplacement.ContainerID{
				{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
				{Namespace: "ns1", PodName: "pod1", ContainerName: "cnt1"},
			},
			wantCount: 1,
		},
	}
}

func runDecoderTests(t *testing.T, tests []decoderTestCase, decode func(dec *Decoder, ci numaplacement.ContainerID) *Decoder) {
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
