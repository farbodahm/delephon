package ui

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/widget"
)

// Node ID format:
//   "p:<project>"
//   "d:<project>/<dataset>"
//   "t:<project>/<dataset>/<table>"

func ProjectNodeID(project string) string           { return "p:" + project }
func DatasetNodeID(project, dataset string) string   { return fmt.Sprintf("d:%s/%s", project, dataset) }
func TableNodeID(project, dataset, table string) string {
	return fmt.Sprintf("t:%s/%s/%s", project, dataset, table)
}

func ParseNodeID(id string) (kind string, project, dataset, table string) {
	if len(id) < 2 {
		return "", "", "", ""
	}
	kind = id[:1]
	rest := id[2:]
	parts := strings.SplitN(rest, "/", 3)
	if len(parts) >= 1 {
		project = parts[0]
	}
	if len(parts) >= 2 {
		dataset = parts[1]
	}
	if len(parts) >= 3 {
		table = parts[2]
	}
	return
}

type LoadChildrenFunc func(nodeID string) ([]string, error)
type OnTableSelectedFunc func(project, dataset, table string)

type Explorer struct {
	widget.Tree

	mu       sync.RWMutex
	children map[string][]string // parent -> child IDs
	roots    []string

	LoadChildren    LoadChildrenFunc
	OnTableSelected OnTableSelectedFunc
}

func NewExplorer() *Explorer {
	e := &Explorer{
		children: make(map[string][]string),
	}

	e.Tree.ChildUIDs = e.childUIDs
	e.Tree.IsBranch = e.isBranch
	e.Tree.CreateNode = e.createNode
	e.Tree.UpdateNode = e.updateNode
	e.Tree.OnSelected = e.onSelected
	e.Tree.OnBranchOpened = e.onBranchOpened

	e.ExtendBaseWidget(e)
	return e
}

func (e *Explorer) SetProjects(projects []string) {
	e.mu.Lock()
	e.roots = nil
	for _, p := range projects {
		id := ProjectNodeID(p)
		e.roots = append(e.roots, id)
	}
	sort.Strings(e.roots)
	e.mu.Unlock()
	e.Refresh()
}

func (e *Explorer) AddProject(project string) {
	e.mu.Lock()
	id := ProjectNodeID(project)
	for _, r := range e.roots {
		if r == id {
			e.mu.Unlock()
			return
		}
	}
	e.roots = append(e.roots, id)
	sort.Strings(e.roots)
	e.mu.Unlock()
	e.Refresh()
}

func (e *Explorer) childUIDs(id widget.TreeNodeID) []widget.TreeNodeID {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if id == "" {
		return e.roots
	}
	return e.children[id]
}

func (e *Explorer) isBranch(id widget.TreeNodeID) bool {
	kind, _, _, _ := ParseNodeID(id)
	return kind == "p" || kind == "d"
}

func (e *Explorer) createNode(branch bool) fyne.CanvasObject {
	return widget.NewLabel("Loading...")
}

func (e *Explorer) updateNode(id widget.TreeNodeID, branch bool, obj fyne.CanvasObject) {
	label := obj.(*widget.Label)
	kind, project, dataset, table := ParseNodeID(id)
	switch kind {
	case "p":
		label.SetText(project)
	case "d":
		label.SetText(dataset)
	case "t":
		label.SetText(table)
	default:
		label.SetText(id)
	}
}

func (e *Explorer) onSelected(id widget.TreeNodeID) {
	kind, project, dataset, table := ParseNodeID(id)
	if kind == "t" && e.OnTableSelected != nil {
		e.OnTableSelected(project, dataset, table)
	}
}

func (e *Explorer) onBranchOpened(id widget.TreeNodeID) {
	e.mu.RLock()
	_, exists := e.children[id]
	e.mu.RUnlock()
	if exists {
		return
	}

	if e.LoadChildren == nil {
		return
	}

	go func() {
		childIDs, err := e.LoadChildren(id)
		if err != nil {
			fmt.Printf("load children error for %s: %v\n", id, err)
			return
		}
		e.mu.Lock()
		e.children[id] = childIDs
		e.mu.Unlock()
		e.Refresh()
	}()
}
