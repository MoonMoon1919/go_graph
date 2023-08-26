package main

import (
	"fmt"
)

type NodeID string
type NodeIDs map[NodeID]struct{}
type SortedNodeIDs []NodeID
type NodeFn func(name NodeID) error

type Node struct {
	Name         string
	Fn           NodeFn
	Dependencies NodeIDs
}

func NewNode(name string, dependencies NodeIDs, fn NodeFn) *Node {
	return &Node{
		Name:         name,
		Fn:           fn,
		Dependencies: dependencies,
	}
}

func (n *Node) Identifier() NodeID {
	return NodeID(n.Name)
}

type Nodes map[NodeID]*Node

type Graph struct {
	name  string
	nodes Nodes
}

func NewGraph(name string) *Graph {
	return &Graph{
		name:  name,
		nodes: make(Nodes),
	}
}

func (g *Graph) Add(node *Node) (NodeID, error) {
	id := node.Identifier()
	if _, ok := g.nodes[id]; ok {
		return "", fmt.Errorf("Node with id %s already exists", id)
	}

	for depId := range node.Dependencies {
		if _, ok := g.nodes[depId]; !ok {
			return "", fmt.Errorf("Node %s is missing dependency %s", id, depId)
		}
	}
	g.nodes[id] = node
	return id, nil
}

func (g *Graph) Sort() (SortedNodeIDs, error) {
	visited := map[NodeID]bool{}
	results := make(SortedNodeIDs, len(g.nodes))

	for n, node := range g.nodes {
		if !visited[n] {
			stack := map[NodeID]bool{}
			if err := g.visit(n, node.Dependencies, stack, visited, results); err != nil {
				return nil, err
			}
		}
	}

	return results, nil
}

func (g *Graph) visit(name NodeID, neighbors NodeIDs, stack map[NodeID]bool, visited map[NodeID]bool, results []NodeID) error {
	visited[name] = true
	stack[name] = true

	for n := range neighbors {
		if !visited[n] {
			// Child node doesn't exist
			if _, ok := g.nodes[n]; !ok {
				return fmt.Errorf("Node %s does not exist", n)
			}

			// Propagate errors from recursive calls
			if err := g.visit(n, g.nodes[n].Dependencies, stack, visited, results); err != nil {
				return err
			}
		} else if stack[n] {
			return fmt.Errorf("Detected cycle on %s", n)
		}
	}

	for i, r := range results {
		if r == "" {
			results[i] = name
			break
		}
	}

	stack[name] = false
	return nil
}

// Make it a parallelize workflow
type ExecutableNode struct {
	targetIDs NodeIDs
	required  int
	fn        NodeFn
}

func (exn *ExecutableNode) AddTargets(nodeIds ...NodeID) {
	if exn.targetIDs == nil {
		targets := make(NodeIDs)

		for nidx := range nodeIds {
			id := nodeIds[nidx]
			targets[id] = struct{}{}
		}

		exn.targetIDs = targets
		return
	}

	// Add to the existing map
	for nidx := range nodeIds {
		id := nodeIds[nidx]
		exn.targetIDs[id] = struct{}{}
	}
}

type executableNodes map[NodeID]*ExecutableNode

func (en executableNodes) RootIds() []NodeID {
	rootIds := []NodeID{}

	for id, node := range en {
		if node.required == 0 {
			rootIds = append(rootIds, id)
		}
	}

	return rootIds
}

func (en executableNodes) GetOrCreate(id NodeID) *ExecutableNode {
	n, ok := en[id]
	if !ok {
		n = &ExecutableNode{}
		en[id] = n
	}
	return n
}

type ParallelizedExecutableGraph struct {
	name  string
	nodes executableNodes
}

func (g *Graph) CompileToExecutable() *ParallelizedExecutableGraph {
	nodes := make(executableNodes, len(g.nodes))

	for id, node := range g.nodes {
		for depId := range node.Dependencies {
			dep := nodes.GetOrCreate(depId)
			dep.AddTargets(id)
		}

		n := nodes.GetOrCreate(id)
		n.fn = node.Fn
		n.required = len(node.Dependencies)
	}

	return &ParallelizedExecutableGraph{
		name:  g.name,
		nodes: nodes,
	}
}

func (peg *ParallelizedExecutableGraph) runNode(id NodeID) {
	node := peg.nodes[id]
	node.fn(id)

	for target := range node.targetIDs {
		peg.runNode(target)
	}
}

func (peg *ParallelizedExecutableGraph) run() {
	rootIds := peg.nodes.RootIds()

	for idx := range rootIds {
		nodeID := rootIds[idx]

		peg.runNode(nodeID)
	}
}

// Driver
func main() {
	g := NewGraph("my-graph")

	doodad := func() NodeFn {
		return func(name NodeID) error {
			fmt.Printf("Running node %s\n", name)
			return nil
		}
	}

	// Create a bunch of new nodes
	n1 := NewNode("a", NodeIDs{}, doodad())
	n3 := NewNode("b", NodeIDs{n1.Identifier(): {}}, doodad())
	n2 := NewNode("c", NodeIDs{n3.Identifier(): {}}, doodad())
	n4 := NewNode("d", NodeIDs{n2.Identifier(): {}}, doodad())
	n5 := NewNode("e", NodeIDs{}, doodad())

	// Add all the nodes
	_, err := g.Add(n1)
	_, err = g.Add(n3)
	_, err = g.Add(n2)
	_, err = g.Add(n4)
	_, err = g.Add(n5)

	if err != nil {
		fmt.Println(err.Error())
	}

	wf := g.CompileToExecutable()
	wf.run()
}
