// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Red Hat, Inc.

package plainmap

import (
	"maps"
	"reflect"
	"slices"
	"strings"

	"github.com/k8stopologyawareschedwg/numaplacement"
)

// We separate container triples (namespace/podName/containerName) within
// a per-NUMA vector string. The pipe character "|" is safe because Kubernetes
// names (namespaces, pod names, container names) are validated as RFC 1123
// labels [a-z0-9-] or DNS subdomains [a-z0-9-.], so "|" can never appear in
// a ContainerID.String() output.
// The triples are guaranteed to be sorted alphabetically.
// See https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#dns-label-names
// and https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#dns-subdomain-names
const (
	VectorEncodingPlain string = "plain"
)

const (
	vecSep = "|"
)

type Encoder struct {
	numaNodes    int
	numaLocality map[string]int
}

// NewEncoder creates a new Encoder object for the given NUMA Nodes count,
// optionally encoding the given ContainerAffinities. Even though it is
// supported, the NUMA Node count should usually never change in the
// lifetime of the program.
// If duplicate Affinities are given (same ID with different Affinity) the
// latest added wins and the previous instances are silently discarded.
func NewEncoder(numaNodes int, cas ...numaplacement.ContainerAffinity) (*Encoder, error) {
	if numaNodes <= 0 {
		return nil, numaplacement.ErrInconsistentNUMANodes
	}
	enc := &Encoder{
		numaNodes:    numaNodes,
		numaLocality: make(map[string]int),
	}
	return enc.Encode(cas...)
}

// Containers return the number of containers this encoder knows about
func (enc *Encoder) Containers() int {
	return len(enc.numaLocality)
}

// NUMANodes return the number of NUMA nodes this encoder knows about
func (enc *Encoder) NUMANodes() int {
	return enc.numaNodes
}

// Encode encodes the given ContainerAffinities.
// If duplicate Affinities are given (same ID with different Affinity) the
// latest added wins and the previous instances are silently discarded.
func (enc *Encoder) Encode(cas ...numaplacement.ContainerAffinity) (*Encoder, error) {
	for _, ca := range cas {
		if ca.NUMANode == numaplacement.UnknownNUMAAffinity {
			return nil, numaplacement.ErrUnsupportedUnknownNUMAAffinity
		}
		if ca.NUMANode != numaplacement.UnknownNUMAAffinity && ca.NUMANode < 0 {
			return nil, numaplacement.ErrWrongNUMAAffinity
		}
		if ca.NUMANode > enc.numaNodes-1 {
			return nil, numaplacement.ErrWrongNUMAAffinity
		}
		enc.numaLocality[ca.ID.String()] = ca.NUMANode
	}
	return enc, nil
}

// EncodeContainer encode a container affinity through its essential attributes.
// If duplicate Affinities are given (same ID with different Affinity) the
// latest added wins and the previous instances are silently discarded.
func (enc *Encoder) EncodeContainer(namespace, podName, containerName string, numaAffinity int) (*Encoder, error) {
	return enc.Encode(numaplacement.ContainerAffinity{
		ID: numaplacement.ContainerID{
			Namespace:     namespace,
			PodName:       podName,
			ContainerName: containerName,
		},
		NUMANode: numaAffinity,
	})
}

// Result finalizes the encoding of a set of ContainerAffinities.
// On failure, the Payload must be ignored and the error is not nil.
func (enc *Encoder) Result() (numaplacement.Payload, error) {
	pl := numaplacement.Payload{
		Containers:     len(enc.numaLocality),
		NUMANodes:      enc.numaNodes,
		BusiestNode:    0,
		VectorEncoding: VectorEncodingPlain,
		Vectors:        make(map[int]string),
	}
	if len(enc.numaLocality) == 0 {
		return pl, nil
	}
	vecs := make(map[int][]string)
	for cid, numaNode := range enc.numaLocality {
		vecs[numaNode] = append(vecs[numaNode], cid)
	}
	for numaNode, vec := range vecs {
		slices.Sort(vec)
		pl.Vectors[numaNode] = strings.Join(vec, vecSep)
	}
	return pl, nil
}

// Info represents compactly-stored NUMA locality information.
// This is the data the consumer side should store and keep up to date.
type PlainInfo struct {
	numaLocality map[string]int // namespace/podName/containerName->numaID
}

func NewPlainInfo() *PlainInfo {
	return &PlainInfo{
		numaLocality: make(map[string]int),
	}
}

// Containers returns the count of the containers we know about.
func (info *PlainInfo) Containers() int {
	return len(info.numaLocality)
}

// Equal returns true if this info has equal value to the given one, false otherwise.
func (info *PlainInfo) Equal(obj *PlainInfo) bool {
	return reflect.DeepEqual(info.numaLocality, obj.numaLocality)
}

// Update clones the numalocality semantics from the given `data`.
func (info *PlainInfo) Update(data *PlainInfo) {
	info.numaLocality = maps.Clone(data.numaLocality)
}

// Take moves the numalocality semantics from the given `data`.
func (info *PlainInfo) Take(data *PlainInfo) {
	info.numaLocality = data.numaLocality
	data.numaLocality = nil
}

// NUMAAffinity returns the NUMA Node mapping of a given ContainerID.
// On failure, the affinity is not relevant and the error is not nil
func (info *PlainInfo) NUMAAffinity(id numaplacement.ContainerID) (int, error) {
	numaNode, ok := info.numaLocality[id.String()]
	if !ok {
		return -1, numaplacement.ErrUnknownContainer
	}
	return numaNode, nil
}

// NUMAAffinityContainer returns the NUMA Node mapping of a given Container by its attributes.
// On failure, the affinity is not relevant and the error is not nil
func (info *PlainInfo) NUMAAffinityContainer(namespace, podName, containerName string) (int, error) {
	return info.NUMAAffinity(numaplacement.ContainerID{Namespace: namespace, PodName: podName, ContainerName: containerName})
}

// Decoder takes a Payload and produces an Info object the client code can use
// to learn the NUMA affinity of a container.
// Likewise the Encoder, containers can be added in a streaming manner, not necessarily in one go,
// but once the Info is created from the Payload, the Decoder instance must be discarded.
type Decoder struct {
	payload numaplacement.Payload
	cntSet  map[string]struct{}
}

func decoderValidate(pl numaplacement.Payload) error {
	if pl.VectorEncoding != VectorEncodingPlain {
		return numaplacement.ErrUnsupportedVectorEncoding
	}
	return nil
}

// NewDecoder creates a new Decoder for the given Payload object, optionally prefeeding with
// the given ContainerIDs.
// If duplicate ContainerIDs are given the latest added wins and the previous IDs are silently discarded.
func NewDecoder(pl numaplacement.Payload, ids ...numaplacement.ContainerID) (*Decoder, error) {
	if err := pl.Validate(decoderValidate); err != nil {
		return nil, err
	}
	dec := &Decoder{
		payload: pl,
		cntSet:  make(map[string]struct{}),
	}
	return dec.Decode(ids...), nil
}

// Containers return the number of containers this decoder knows about.
func (dec *Decoder) Containers() int {
	return len(dec.cntSet)
}

// Decode adds the given set of ContainerIDs to the decoder.
// In plainmap, this is a no-op since the payload carries triples directly.
func (dec *Decoder) Decode(ids ...numaplacement.ContainerID) *Decoder {
	for _, id := range ids {
		dec.cntSet[id.String()] = struct{}{}
	}
	return dec
}

// DecodeContainer adds the given container through its essential attributes.
// In plainmap, this is a no-op since the payload carries triples directly.
func (dec *Decoder) DecodeContainer(namespace, podName, containerName string) *Decoder {
	return dec.Decode(numaplacement.ContainerID{
		Namespace:     namespace,
		PodName:       podName,
		ContainerName: containerName,
	})
}

// Result finalizes a Decoder and returns Info object the client code can consume to query the
// affinity of the given ContainerID set from the Payload. On failure, error is not nil and
// the Info must be ignored (it is usually `nil`, but is not guaranteed to be `nil`).
func (dec *Decoder) Result() (numaplacement.Info, error) {
	if len(dec.cntSet) != dec.payload.Containers {
		return nil, numaplacement.ErrInconsistentContainerSet
	}
	// Structural invariants are already validated by Payload.Validate() in NewDecoder.
	// Plain encoding includes all containers in vectors (no busiest-node optimization),
	// so no reconstruction is needed.
	info := &PlainInfo{
		numaLocality: make(map[string]int),
	}
	for numaNode, cidData := range dec.payload.Vectors {
		rest := cidData
		for rest != "" {
			cid, after, found := strings.Cut(rest, vecSep)
			if cid == "" {
				return nil, numaplacement.ErrCorruptedNUMAVector
			}
			if _, ok := dec.cntSet[cid]; !ok {
				return nil, numaplacement.ErrCorruptedNUMAVector
			}
			if _, ok := info.numaLocality[cid]; ok {
				return nil, numaplacement.ErrDuplicatedNUMAVector
			}
			info.numaLocality[cid] = numaNode
			if !found {
				break
			}
			if after == "" {
				return nil, numaplacement.ErrCorruptedNUMAVector
			}
			rest = after
		}
	}
	return info, nil
}
