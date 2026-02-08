package ui

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
)

// Node ID format:
//   "p:<project>"
//   "d:<project>/<dataset>"
//   "t:<project>/<dataset>/<table>"

func ProjectNodeID(project string) string         { return "p:" + project }
func DatasetNodeID(project, dataset string) string { return fmt.Sprintf("d:%s/%s", project, dataset) }
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
	Tree *widget.Tree

	mu       sync.RWMutex
	children map[string][]string // parent -> child IDs
	roots    []string
	loading  map[string]bool // tracks in-flight loads

	LoadChildren    LoadChildrenFunc
	OnTableSelected OnTableSelectedFunc

	// Favorite projects
	favList       *widget.List
	favProjects   []string
	OnFavSelected func(project string)

	Container fyne.CanvasObject
}

func NewExplorer() *Explorer {
	e := &Explorer{
		children: make(map[string][]string),
		loading:  make(map[string]bool),
	}

	e.Tree = widget.NewTree(
		e.childUIDs,
		e.isBranch,
		e.createNode,
		e.updateNode,
	)
	e.Tree.OnSelected = e.onSelected
	e.Tree.OnBranchOpened = e.onBranchOpened

	// Favorite projects list
	e.favList = widget.NewList(
		func() int { return len(e.favProjects) },
		func() fyne.CanvasObject { return widget.NewLabel("") },
		func(id widget.ListItemID, obj fyne.CanvasObject) {
			if id < len(e.favProjects) {
				obj.(*widget.Label).SetText(e.favProjects[id])
			}
		},
	)
	e.favList.OnSelected = func(id widget.ListItemID) {
		if id < len(e.favProjects) && e.OnFavSelected != nil {
			e.OnFavSelected(e.favProjects[id])
		}
		e.favList.UnselectAll()
	}

	favLabel := widget.NewLabel("Favorite Projects")
	favLabel.TextStyle = fyne.TextStyle{Bold: true}
	favSection := container.NewBorder(favLabel, nil, nil, nil, e.favList)

	e.Container = container.NewVSplit(e.Tree, favSection)
	e.Container.(*container.Split).Offset = 0.7

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
	e.Tree.Refresh()
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
	e.Tree.Refresh()
}

func (e *Explorer) SetFavProjects(projects []string) {
	e.mu.Lock()
	e.favProjects = projects
	e.mu.Unlock()
	e.favList.Refresh()
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
	e.mu.Lock()
	if _, exists := e.children[id]; exists {
		e.mu.Unlock()
		return
	}
	if e.loading[id] {
		e.mu.Unlock()
		return
	}
	e.loading[id] = true
	e.mu.Unlock()

	if e.LoadChildren == nil {
		return
	}

	go func() {
		childIDs, err := e.LoadChildren(id)

		e.mu.Lock()
		delete(e.loading, id)
		e.mu.Unlock()

		if err != nil {
			fmt.Printf("load children error for %s: %v\n", id, err)
			return
		}

		e.mu.Lock()
		e.children[id] = childIDs
		e.mu.Unlock()

		// Re-open the branch so the tree queries children again
		e.Tree.OpenBranch(id)
	}()
}
