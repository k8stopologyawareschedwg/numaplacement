// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Red Hat, Inc.

package main

import (
	"fmt"
	"log"

	"github.com/k8stopologyawareschedwg/numaplacement"
)

func main() {
	affinities := []numaplacement.ContainerAffinity{
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
			NUMANode: 1,
		},
		{
			ID: numaplacement.ContainerID{
				Namespace:     "default",
				PodName:       "workload-b",
				ContainerName: "app",
			},
			NUMANode: 0,
		},
	}

	encoder, err := numaplacement.NewEncoder(2, affinities...)
	if err != nil {
		log.Fatalf("create encoder: %v", err)
	}

	payload, err := encoder.Result()
	if err != nil {
		log.Fatalf("finalize payload: %v", err)
	}

	fmt.Printf("encoded metadata: %s\n", payload.PackMetadata())

	containerIDs := make([]numaplacement.ContainerID, 0, len(affinities))
	for _, ca := range affinities {
		containerIDs = append(containerIDs, ca.ID)
	}

	decoder, err := numaplacement.NewDecoder(payload, containerIDs...)
	if err != nil {
		log.Fatalf("create decoder: %v", err)
	}

	info, err := decoder.Result()
	if err != nil {
		log.Fatalf("decode payload: %v", err)
	}

	fmt.Println("decoded info:")
	for _, id := range containerIDs {
		numaNode, err := info.NUMAAffinity(id)
		if err != nil {
			log.Fatalf("query %s: %v", id.String(), err)
		}
		fmt.Printf("%s -> NUMA %d\n", id.String(), numaNode)
	}
}
