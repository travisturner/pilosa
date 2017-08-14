// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pilosa

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"reflect"
	"time"

	"github.com/pilosa/pilosa/internal"
)

const (
	// DefaultPartitionN is the default number of partitions in a cluster.
	DefaultPartitionN = 256

	// DefaultReplicaN is the default number of replicas per partition.
	DefaultReplicaN = 1
)

// NodeState represents node state returned in /status endpoint for a node in the cluster.
const (
	NodeStateStarting = "STARTING"
	NodeStateUp       = "UP" // TODO travis remove this
	NodeStateDown     = "DOWN"
	NodeStateNormal   = "NORMAL"
	NodeStateSettling = "SETTLING"
	NodeStateJoining  = "JOINING"
	NodeStateLeaving  = "LEAVING"

	NodeStateTaxonomyLoaded    = "TAXONOMY-LOADED"
	NodeStateTaxonomyConfirmed = "TAXONOMY-CONFIRMED"
	NodeStateRebalancing       = "REBALANCING"
)

// Node represents a node in the cluster.
type Node struct {
	Host string `json:"host"`

	status *internal.NodeStatus `json:"status"`
}

// SetStatus sets the NodeStatus.
func (n *Node) SetStatus(s *internal.NodeStatus) bool {
	if n.status == nil {
		n.status = s
		return true
	}

	changed := false
	if n.status.State != s.State {
		changed = true
	}
	if !reflect.DeepEqual(n.status.HostList, s.HostList) {
		changed = true
	}
	n.status = s
	return changed
}

// TODO travis: maybe we need a Server.SetState() too, that calls this?
// because we're maintaining Server.State as well as Server.Cluster.Nodes(node).State
// and setting the state for self (i.e. Server) should update Cluster.Nodes as well
// SetState sets the Node.status.state. Returns true if state changed.
func (n *Node) SetState(s string) bool {
	changed := false
	if n.status == nil {
		n.status = &internal.NodeStatus{}
	}
	if n.status.State != s {
		n.status.State = s
		changed = true
	}
	return changed
}

// SetHostList sets the host list according to the node.
func (n *Node) SetHostList(h []string) bool {
	changed := false
	if n.status == nil {
		n.status = &internal.NodeStatus{}
	}
	if !reflect.DeepEqual(n.status.HostList, h) {
		n.status.HostList = h
		changed = true
	}
	return changed
}

// Nodes represents a list of nodes.
type Nodes []*Node

// Contains returns true if a node exists in the list.
func (a Nodes) Contains(n *Node) bool {
	for i := range a {
		if a[i] == n {
			return true
		}
	}
	return false
}

// ContainsHost returns true if host matches one of the node's host.
func (a Nodes) ContainsHost(host string) bool {
	for _, n := range a {
		if n.Host == host {
			return true
		}
	}
	return false
}

// Filter returns a new list of nodes with node removed.
func (a Nodes) Filter(n *Node) []*Node {
	other := make([]*Node, 0, len(a))
	for i := range a {
		if a[i] != n {
			other = append(other, a[i])
		}
	}
	return other
}

// FilterHost returns a new list of nodes with host removed.
func (a Nodes) FilterHost(host string) []*Node {
	other := make([]*Node, 0, len(a))
	for _, node := range a {
		if node.Host != host {
			other = append(other, node)
		}
	}
	return other
}

// Hosts returns a list of all hostnames. // TODO travis: check this
func (a Nodes) Hosts() []string {
	hosts := make([]string, len(a))
	for i, n := range a {
		hosts[i] = n.Host
	}
	return hosts
}

// StatesMatch returns true if the state for every node is the same.
func (a Nodes) StatesMatch() bool {
	var compare string
	for i, n := range a {
		if i == 0 {
			compare = n.status.State
			continue
		}
		if compare != n.status.State {
			return false
		}
	}
	return true
}

// HostListsMatch returns true if the hostlist for every node is the same.
func (a Nodes) HostListsMatch() bool {
	var compare []string
	for i, n := range a {
		if i == 0 {
			compare = n.status.HostList
			continue
		}
		if !reflect.DeepEqual(n.status.HostList, compare) {
			return false
		}
	}
	return true
}

// StatesIn returns true if the state for every node is in options.
func (a Nodes) StatesIn(options []string) bool {
	for _, n := range a {
		if !StringInSlice(n.status.State, options) {
			return false
		}
	}
	return true
}

// Clone returns a shallow copy of nodes.
func (a Nodes) Clone() []*Node {
	other := make([]*Node, len(a))
	copy(other, a)
	return other
}

// ByHost implements sort.Interface for []Node based on
// the Host field.
type ByHost []*Node

func (h ByHost) Len() int           { return len(h) }
func (h ByHost) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h ByHost) Less(i, j int) bool { return h[i].Host < h[j].Host }

// Cluster represents a collection of nodes.
type Cluster struct {
	Nodes   []*Node // TODO travis phase this out?
	NodeSet NodeSet

	// Hashing algorithm used to assign partitions to nodes.
	Hasher Hasher

	// The number of partitions in the cluster.
	PartitionN int

	// The number of replicas a partition has.
	ReplicaN int

	// Threshold for logging long-running queries
	LongQueryTime time.Duration
}

// NewCluster returns a new instance of Cluster with defaults.
func NewCluster() *Cluster {
	return &Cluster{
		Hasher:     &jmphasher{},
		PartitionN: DefaultPartitionN,
		ReplicaN:   DefaultReplicaN,
	}
}

// TODO travis
func (c *Cluster) Debug() string {
	r := ""
	for i, n := range c.Nodes {
		r += fmt.Sprintf("\n --- node%v: %s (%s) <%v>", i, n.Host, n.status.State, n.status.HostList)
	}
	return r
}

// NodeSetHosts returns the list of host strings for NodeSet members.
func (c *Cluster) NodeSetHosts() []string {
	if c.NodeSet == nil {
		return []string{}
	}
	a := make([]string, 0, len(c.NodeSet.Nodes()))
	for _, m := range c.NodeSet.Nodes() {
		a = append(a, m.Host)
	}
	return a
}

// NodeStates returns a map of nodes in the cluster with each node's state (UP/DOWN) as the value.
func (c *Cluster) NodeStates() map[string]string {
	h := make(map[string]string)
	for _, n := range c.Nodes {
		h[n.Host] = NodeStateDown
	}
	// we are assuming that NodeSetHosts is a subset of c.Nodes
	for _, m := range c.NodeSetHosts() {
		if _, ok := h[m]; ok {
			h[m] = NodeStateUp
		}
	}
	return h
}

// Status returns the internal ClusterStatus representation.
func (c *Cluster) Status() *internal.ClusterStatus {
	return &internal.ClusterStatus{
		Nodes: encodeClusterStatus(c.Nodes),
	}
}

// encodeClusterStatus converts a into its internal representation.
func encodeClusterStatus(a []*Node) []*internal.NodeStatus {
	other := make([]*internal.NodeStatus, len(a))
	for i := range a {
		other[i] = a[i].status
	}
	return other
}

// NodeByHost returns a node reference by host.
func (c *Cluster) NodeByHost(host string) *Node {
	for _, n := range c.Nodes {
		if n.Host == host {
			return n
		}
	}
	return nil
}

// Partition returns the partition that a slice belongs to.
func (c *Cluster) Partition(index string, slice uint64) int {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], slice)

	// Hash the bytes and mod by partition count.
	h := fnv.New64a()
	h.Write([]byte(index))
	h.Write(buf[:])
	return int(h.Sum64() % uint64(c.PartitionN))
}

// FragmentNodes returns a list of nodes that own a fragment.
func (c *Cluster) FragmentNodes(index string, slice uint64) []*Node {
	return c.PartitionNodes(c.Partition(index, slice))
}

// OwnsFragment returns true if a host owns a fragment.
func (c *Cluster) OwnsFragment(host string, index string, slice uint64) bool {
	return Nodes(c.FragmentNodes(index, slice)).ContainsHost(host)
}

// PartitionNodes returns a list of nodes that own a partition.
func (c *Cluster) PartitionNodes(partitionID int) []*Node {
	// Default replica count to between one and the number of nodes.
	// The replica count can be zero if there are no nodes.
	replicaN := c.ReplicaN
	if replicaN > len(c.Nodes) {
		replicaN = len(c.Nodes)
	} else if replicaN == 0 {
		replicaN = 1
	}

	// Determine primary owner node.
	nodeIndex := c.Hasher.Hash(uint64(partitionID), len(c.Nodes))

	// Collect nodes around the ring.
	nodes := make([]*Node, replicaN)
	for i := 0; i < replicaN; i++ {
		nodes[i] = c.Nodes[(nodeIndex+i)%len(c.Nodes)]
	}

	return nodes
}

// OwnsSlices find the set of slices owned by the node per Index
func (c *Cluster) OwnsSlices(index string, maxSlice uint64, host string) []uint64 {
	var slices []uint64
	for i := uint64(0); i <= maxSlice; i++ {
		p := c.Partition(index, i)
		// Determine primary owner node.
		nodeIndex := c.Hasher.Hash(uint64(p), len(c.Nodes))
		if c.Nodes[nodeIndex].Host == host {
			slices = append(slices, i)
		}
	}
	return slices
}

// Hasher represents an interface to hash integers into buckets.
type Hasher interface {
	// Hashes the key into a number between [0,N).
	Hash(key uint64, n int) int
}

// NewHasher returns a new instance of the default hasher.
func NewHasher() Hasher { return &jmphasher{} }

// jmphasher represents an implementation of jmphash. Implements Hasher.
type jmphasher struct{}

// Hash returns the integer hash for the given key.
func (h *jmphasher) Hash(key uint64, n int) int {
	b, j := int64(-1), int64(0)
	for j < int64(n) {
		b = j
		key = key*uint64(2862933555777941757) + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}
	return int(b)
}
