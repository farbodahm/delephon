package ui

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/theme"
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

type explorerNode struct {
	id       string
	label    string
	depth    int  // 0=project, 1=dataset, 2=table
	isBranch bool
	expanded bool
}

type Explorer struct {
	list *widget.List

	mu       sync.Mutex
	visible  []explorerNode            // flat list of currently visible nodes
	children map[string][]explorerNode // parent id -> loaded children
	loading  map[string]bool

	LoadChildren    LoadChildrenFunc
	OnTableSelected OnTableSelectedFunc

	// Favorite projects
	favList     *widget.List
	favProjects []string
	OnFavSelected func(project string)

	Container fyne.CanvasObject
}

func NewExplorer() *Explorer {
	e := &Explorer{
		children: make(map[string][]explorerNode),
		loading:  make(map[string]bool),
	}

	e.list = widget.NewList(
		func() int {
			e.mu.Lock()
			defer e.mu.Unlock()
			return len(e.visible)
		},
		func() fyne.CanvasObject {
			spacer := widget.NewLabel("")        // used for indentation width
			icon := widget.NewIcon(theme.NavigateNextIcon())
			label := widget.NewLabel("template")
			return container.NewHBox(spacer, icon, label)
		},
		func(id widget.ListItemID, obj fyne.CanvasObject) {
			e.mu.Lock()
			if id >= len(e.visible) {
				e.mu.Unlock()
				return
			}
			node := e.visible[id]
			e.mu.Unlock()

			box := obj.(*fyne.Container)
			spacer := box.Objects[0].(*widget.Label)
			icon := box.Objects[1].(*widget.Icon)
			label := box.Objects[2].(*widget.Label)

			// Indentation: repeat spaces based on depth
			indent := ""
			for i := 0; i < node.depth; i++ {
				indent += "    "
			}
			spacer.SetText(indent)

			label.SetText(node.label)

			if node.isBranch {
				if node.expanded {
					icon.SetResource(theme.MoveDownIcon())
				} else {
					icon.SetResource(theme.NavigateNextIcon())
				}
			} else {
				icon.SetResource(theme.DocumentIcon())
			}
		},
	)

	e.list.OnSelected = func(id widget.ListItemID) {
		e.list.UnselectAll()
		e.mu.Lock()
		if id >= len(e.visible) {
			e.mu.Unlock()
			return
		}
		node := e.visible[id]
		e.mu.Unlock()

		if node.isBranch {
			e.toggleBranch(node.id)
		} else {
			kind, project, dataset, table := ParseNodeID(node.id)
			if kind == "t" && e.OnTableSelected != nil {
				e.OnTableSelected(project, dataset, table)
			}
		}
	}

	// Favorite projects list
	e.favList = widget.NewList(
		func() int {
			e.mu.Lock()
			defer e.mu.Unlock()
			return len(e.favProjects)
		},
		func() fyne.CanvasObject { return widget.NewLabel("") },
		func(id widget.ListItemID, obj fyne.CanvasObject) {
			e.mu.Lock()
			if id < len(e.favProjects) {
				obj.(*widget.Label).SetText(e.favProjects[id])
			}
			e.mu.Unlock()
		},
	)
	e.favList.OnSelected = func(id widget.ListItemID) {
		e.mu.Lock()
		if id < len(e.favProjects) {
			p := e.favProjects[id]
			e.mu.Unlock()
			if e.OnFavSelected != nil {
				e.OnFavSelected(p)
			}
		} else {
			e.mu.Unlock()
		}
		e.favList.UnselectAll()
	}

	favLabel := widget.NewLabel("Favorite Projects")
	favLabel.TextStyle = fyne.TextStyle{Bold: true}
	favSection := container.NewBorder(favLabel, nil, nil, nil, e.favList)

	split := container.NewVSplit(e.list, favSection)
	split.Offset = 0.7
	e.Container = split

	return e
}

func (e *Explorer) SetProjects(projects []string) {
	e.mu.Lock()
	e.visible = nil
	for _, p := range projects {
		e.visible = append(e.visible, explorerNode{
			id:       ProjectNodeID(p),
			label:    p,
			depth:    0,
			isBranch: true,
		})
	}
	e.mu.Unlock()
	fyne.Do(func() { e.list.Refresh() })
}

func (e *Explorer) AddProject(project string) {
	e.mu.Lock()
	id := ProjectNodeID(project)
	for _, n := range e.visible {
		if n.id == id {
			e.mu.Unlock()
			return
		}
	}
	e.visible = append(e.visible, explorerNode{
		id:       id,
		label:    project,
		depth:    0,
		isBranch: true,
	})
	// Sort projects
	sort.Slice(e.visible, func(i, j int) bool {
		if e.visible[i].depth != e.visible[j].depth {
			return false // don't re-sort nested items
		}
		return e.visible[i].label < e.visible[j].label
	})
	e.mu.Unlock()
	fyne.Do(func() { e.list.Refresh() })
}

func (e *Explorer) SetFavProjects(projects []string) {
	e.mu.Lock()
	e.favProjects = projects
	e.mu.Unlock()
	fyne.Do(func() { e.favList.Refresh() })
}

func (e *Explorer) toggleBranch(id string) {
	e.mu.Lock()

	// Find the node index
	idx := -1
	for i, n := range e.visible {
		if n.id == id {
			idx = i
			break
		}
	}
	if idx < 0 {
		e.mu.Unlock()
		return
	}

	node := &e.visible[idx]

	if node.expanded {
		// Collapse: remove children from visible list
		node.expanded = false
		removeCount := e.countDescendants(idx)
		if removeCount > 0 {
			e.visible = append(e.visible[:idx+1], e.visible[idx+1+removeCount:]...)
		}
		e.mu.Unlock()
		fyne.Do(func() { e.list.Refresh() })
		return
	}

	// Expand
	if cached, ok := e.children[id]; ok {
		// Already loaded — insert into visible
		node.expanded = true
		e.insertChildren(idx, cached)
		e.mu.Unlock()
		fyne.Do(func() { e.list.Refresh() })
		return
	}

	// Need to load
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
		log.Printf("explorer: loading children for %s", id)
		childIDs, err := e.LoadChildren(id)

		e.mu.Lock()
		delete(e.loading, id)

		if err != nil {
			e.mu.Unlock()
			log.Printf("explorer: error loading children for %s: %v", id, err)
			return
		}

		log.Printf("explorer: loaded %d children for %s", len(childIDs), id)

		// Build child nodes
		childNodes := make([]explorerNode, len(childIDs))
		for i, cid := range childIDs {
			ckind, _, dataset, table := ParseNodeID(cid)
			label := cid
			isBranch := false
			depth := 0
			switch ckind {
			case "d":
				label = dataset
				isBranch = true
				depth = 1
			case "t":
				label = table
				depth = 2
			}
			childNodes[i] = explorerNode{
				id:       cid,
				label:    label,
				depth:    depth,
				isBranch: isBranch,
			}
		}

		e.children[id] = childNodes

		// Find the node again and expand
		for i := range e.visible {
			if e.visible[i].id == id {
				e.visible[i].expanded = true
				e.insertChildren(i, childNodes)
				break
			}
		}

		e.mu.Unlock()
		fyne.Do(func() { e.list.Refresh() })
	}()
}

// countDescendants returns how many items after idx belong as descendants.
// Must be called with e.mu held.
func (e *Explorer) countDescendants(idx int) int {
	parentDepth := e.visible[idx].depth
	count := 0
	for i := idx + 1; i < len(e.visible); i++ {
		if e.visible[i].depth <= parentDepth {
			break
		}
		count++
	}
	return count
}

// insertChildren inserts childNodes after idx in the visible list.
// Must be called with e.mu held.
func (e *Explorer) insertChildren(idx int, childNodes []explorerNode) {
	if len(childNodes) == 0 {
		return
	}
	tail := make([]explorerNode, len(e.visible[idx+1:]))
	copy(tail, e.visible[idx+1:])
	e.visible = append(e.visible[:idx+1], childNodes...)
	e.visible = append(e.visible, tail...)
}

