// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Red Hat, Inc.

// Package numaplacement efficiently encodes the per-NUMA placement of
// containers with exclusively assigned resources, so they can themselves
// be considered "affine" to a NUMA node.
//
// The encoding uses LEB89, a self-delimiting variable-length encoding that
// maps non-negative integers to sequences of printable ASCII characters.
// See leb89/doc.go for details.
//
// Both the node agent and the scheduler independently produce the same
// deterministic ordered list of container hashes, and per-NUMA
// offset vectors reference positions in that list.
// This package is expected to be in concert with the noderesourcetopology-api
// kubernetes datatype, but does not depend on it.

package numaplacement

import "github.com/cespare/xxhash/v2"

const (
	// AttributeMetadata is the NRT top-level attribute declaring the version
	// of the container offsets and how the data is constructed
	AttributeMetadata = "topology.node.k8s.io/numaplacement-metadata"

	// AttributeVector is the NRT per-zone attribute carrying the
	// LEB89-encoded delta vector of container offsets placed on that NUMA.
	AttributeVector = "topology.node.k8s.io/numaplacement-vector"
)

const (
	// Prefix is the string common to all the fingerprints
	// A prefix is always 4 bytes long
	Prefix = "npv0"
	// Version is the version of this fingerprint. You should
	// only compare compatible versions.
	// A Version is always 4 bytes long, in the form v\X\X\X
	Version = "v001"
)

type Hasher interface {
	Reset()
	WriteString(s string)
	Sum64() uint64
}

type ContainerID struct {
	Namespace     string
	PodName       string
	ContainerName string
}

func (ci ContainerID) String() string {
	return ci.Namespace + "/" + ci.PodName + "/" + ci.ContainerName
}

func (ci ContainerID) HashWith(hasher Hasher) uint64 {
	hasher.Reset()
	hasher.WriteString(ci.Namespace)
	hasher.WriteString("\x00")
	hasher.WriteString(ci.PodName)
	hasher.WriteString("\x00")
	hasher.WriteString(ci.ContainerName)
	return hasher.Sum64()
}

func (ci ContainerID) Hash() uint64 {
	return ci.HashWith(xxhash.New())
}

type ContainerAffinity struct {
	ID       ContainerID
	NUMANode int
}

// Payload is the encoder output which can be added to or derived from the NRT content
// Payload represents (a slightly abstracted) wire data because this package wants
// to avoid direct manipulation, therefore direct dependency, on NRT objects.
// Furthermore, abstracting the payload from the data format allows us to generalize
// easily the applicability of this package.
type Payload struct {
	// Number of containers this info represents
	Containers int
	// Number of NUMA nodes
	NUMANodes int
	// Index of busiest NUMA node, therefore omitted on wire
	BusiestNode int
	// map NUMANodeID -> LEB89-encoded placement vector string
	Vectors map[int]string
}

func UnpackMetadataInto(pl *Payload, metadata string) {}

func (pl Payload) PackMetadata() string {
	return ""
}

// Encoder handles a full set of containers by their name, and their affinity,
// and produces a Payload which compactly represents the affinities vectors.
// Containers can be added in a streaming manner, not necessarily in one go,
// but once the Payload is created, the Encoder instance must be discarded.
type Encoder struct {
	hashes       []uint64
	numaLocality map[uint64]int // hash->numaID
	hasher       *xxhash.Digest
	numaNodes    int
}

func NewEncoder(numaNodes int, cas ...ContainerAffinity) *Encoder {
	return &Encoder{
		numaNodes:    numaNodes,
		hasher:       xxhash.New(),
		numaLocality: make(map[uint64]int),
	}
}

func (enc *Encoder) Encode(cas ...ContainerAffinity) *Encoder {
	return nil
}

func (enc *Encoder) EncodeContainer(namespace, podName, containerName string, numaAffinity int) *Encoder {
	return enc.Encode(ContainerAffinity{
		ID: ContainerID{
			Namespace:     namespace,
			PodName:       podName,
			ContainerName: containerName,
		},
		NUMANode: numaAffinity,
	})
}

func (enc *Encoder) Result() (Payload, error) {
	return Payload{}
}

// Info represents compactly-stored NUMA locality information.
// This is the data the consumer side should store and keep up to date.
type Info struct{}

func (info Info) NUMAAffinity(id ContainerID) (int, error) {
	return -1, nil
}

func (info Info) NUMAAffinityContainer(namespace, podName, containerName string) (int, error) {
	return info.NUMAAffinity(ContainerID{
		Namespace:     namespace,
		PodName:       podName,
		ContainerName: containerName,
	})
}

// Decoder takes a Payload (once, fixed) and needs the full set of expected ContainerIDs
// which must be verified to be consistent with the encoding side at time of the Payload was generated
// And produces a Info object which the client code can use to learn the NUMA affinity of a container.
// Likewise the Encoder, containers can be added in a streaming manner, not necessarily in one go,
// but once the Info is created from the Payload, the Decoder instance must be discarded.
type Decoder struct {
	payload Payload
	hashes  []uint64
	hasher  *xxhash.Digest
}

func NewDecoder(pl Payload, ids ...ContainerID) *Decoder {
	dec := &Decoder{
		payload: Payload,
		hasher:  xxhash.New(),
	}
	return dec.Decode(ids...)
}

func (dec *Decoder) Decode(ids ...ContainerID) *Decoder {
	for _, id := range ids {
		dec.hashes = append(dec.hashes, id.HashWith(dec.hasher))
	}
	return dec
}

func (dec *Decoder) Result() (Info, error) {
	return Info{}, nil
}
