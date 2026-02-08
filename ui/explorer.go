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
	depth    int  // 0=project/header, 1=dataset, 2=table
	isBranch bool
	expanded bool
	isHeader bool // section header (non-clickable for expand, but "All Projects" is clickable to load)
}

const (
	headerFavorites = "header:favorites"
	headerRecent    = "header:recent"
	headerAll       = "header:all"
)

type Explorer struct {
	list        *widget.List
	searchEntry *widget.Entry

	mu       sync.Mutex
	visible  []explorerNode            // flat list of currently visible nodes
	children map[string][]explorerNode // parent id -> loaded children
	loading  map[string]bool

	// Data sources
	favProjects    []string // starred projects
	recentProjects []string // from history
	allProjects    []string // from GCP API (nil until loaded)
	allLoaded      bool     // whether "All Projects" has been fetched

	// Section collapsed state
	favExpanded    bool
	recentExpanded bool
	allExpanded    bool

	searchFilter string // current search text

	LoadChildren       LoadChildrenFunc
	OnTableSelected    OnTableSelectedFunc
	OnLoadAllProjects  func()                // callback to load all projects from GCP
	OnProjectSelected  func(project string)  // callback when a project node is clicked (set in editor)

	Container fyne.CanvasObject
}

func NewExplorer() *Explorer {
	e := &Explorer{
		children:       make(map[string][]explorerNode),
		loading:        make(map[string]bool),
		favExpanded:    true,
		recentExpanded: true,
		allExpanded:    false,
	}

	e.searchEntry = widget.NewEntry()
	e.searchEntry.SetPlaceHolder("Filter projects...")
	e.searchEntry.OnChanged = func(text string) {
		e.mu.Lock()
		e.searchFilter = text
		shouldLoad := text != "" && !e.allLoaded
		e.mu.Unlock()
		if shouldLoad && e.OnLoadAllProjects != nil {
			e.OnLoadAllProjects()
		}
		e.rebuildVisible()
	}

	e.list = widget.NewList(
		func() int {
			e.mu.Lock()
			defer e.mu.Unlock()
			return len(e.visible)
		},
		func() fyne.CanvasObject {
			spacer := widget.NewLabel("")
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

			// Indentation
			indent := ""
			for i := 0; i < node.depth; i++ {
				indent += "    "
			}
			spacer.SetText(indent)

			if node.isHeader {
				label.SetText(node.label)
				label.TextStyle = fyne.TextStyle{Bold: true}
				label.Refresh()
				if node.expanded {
					icon.SetResource(theme.MoveDownIcon())
				} else {
					icon.SetResource(theme.NavigateNextIcon())
				}
				return
			}

			label.SetText(node.label)
			label.TextStyle = fyne.TextStyle{}
			label.Refresh()

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

		if node.isHeader {
			switch node.id {
			case headerFavorites:
				e.mu.Lock()
				e.favExpanded = !e.favExpanded
				e.mu.Unlock()
				e.rebuildVisible()
			case headerRecent:
				e.mu.Lock()
				e.recentExpanded = !e.recentExpanded
				e.mu.Unlock()
				e.rebuildVisible()
			case headerAll:
				e.mu.Lock()
				if !e.allLoaded {
					e.allExpanded = true
					e.mu.Unlock()
					if e.OnLoadAllProjects != nil {
						e.OnLoadAllProjects()
					}
				} else {
					e.allExpanded = !e.allExpanded
					e.mu.Unlock()
					e.rebuildVisible()
				}
			}
			return
		}

		if node.isBranch {
			// If it's a project node, also notify project selection
			kind, project, _, _ := ParseNodeID(node.id)
			if kind == "p" && e.OnProjectSelected != nil {
				e.OnProjectSelected(project)
			}
			e.toggleBranch(node.id)
		} else {
			kind, project, dataset, table := ParseNodeID(node.id)
			if kind == "t" && e.OnTableSelected != nil {
				e.OnTableSelected(project, dataset, table)
			}
		}
	}

	e.Container = container.NewBorder(e.searchEntry, nil, nil, nil, e.list)

	return e
}

// rebuildVisible reconstructs the visible list from the three data sources,
// applying search filter. Must NOT hold e.mu when calling.
func (e *Explorer) rebuildVisible() {
	e.mu.Lock()

	filter := strings.ToLower(e.searchFilter)

	// Save expanded states and children so we can preserve them
	expandedSet := make(map[string]bool)
	for _, n := range e.visible {
		if n.expanded {
			expandedSet[n.id] = true
		}
	}

	var nodes []explorerNode

	if filter != "" {
		// Search mode: flat list of all matching projects
		seen := make(map[string]bool)
		var matches []string

		for _, p := range e.favProjects {
			if strings.Contains(strings.ToLower(p), filter) && !seen[p] {
				seen[p] = true
				matches = append(matches, p)
			}
		}
		for _, p := range e.recentProjects {
			if strings.Contains(strings.ToLower(p), filter) && !seen[p] {
				seen[p] = true
				matches = append(matches, p)
			}
		}
		for _, p := range e.allProjects {
			if strings.Contains(strings.ToLower(p), filter) && !seen[p] {
				seen[p] = true
				matches = append(matches, p)
			}
		}

		for _, p := range matches {
			nid := ProjectNodeID(p)
			node := explorerNode{
				id:       nid,
				label:    p,
				depth:    0,
				isBranch: true,
				expanded: expandedSet[nid],
			}
			nodes = append(nodes, node)
			if node.expanded {
				if cached, ok := e.children[nid]; ok {
					nodes = e.appendExpandedChildren(nodes, nid, cached, expandedSet)
				}
			}
		}
	} else {
		// Section mode
		// Favorites section
		if len(e.favProjects) > 0 {
			nodes = append(nodes, explorerNode{
				id:       headerFavorites,
				label:    "\u2605 Favorite Projects",
				isHeader: true,
				expanded: e.favExpanded,
			})
			if e.favExpanded {
				for _, p := range e.favProjects {
					nid := ProjectNodeID(p)
					node := explorerNode{
						id:       nid,
						label:    p,
						depth:    0,
						isBranch: true,
						expanded: expandedSet[nid],
					}
					nodes = append(nodes, node)
					if node.expanded {
						if cached, ok := e.children[nid]; ok {
							nodes = e.appendExpandedChildren(nodes, nid, cached, expandedSet)
						}
					}
				}
			}
		}

		// Recent section
		if len(e.recentProjects) > 0 {
			nodes = append(nodes, explorerNode{
				id:       headerRecent,
				label:    "\u23F1 Recent Projects",
				isHeader: true,
				expanded: e.recentExpanded,
			})
			if e.recentExpanded {
				for _, p := range e.recentProjects {
					nid := ProjectNodeID(p)
					node := explorerNode{
						id:       nid,
						label:    p,
						depth:    0,
						isBranch: true,
						expanded: expandedSet[nid],
					}
					nodes = append(nodes, node)
					if node.expanded {
						if cached, ok := e.children[nid]; ok {
							nodes = e.appendExpandedChildren(nodes, nid, cached, expandedSet)
						}
					}
				}
			}
		}

		// All Projects section
		allLabel := "All Projects"
		if !e.allLoaded {
			allLabel = "All Projects (click to load)"
		}
		nodes = append(nodes, explorerNode{
			id:       headerAll,
			label:    allLabel,
			isHeader: true,
			expanded: e.allExpanded,
		})
		if e.allExpanded && e.allLoaded {
			for _, p := range e.allProjects {
				nid := ProjectNodeID(p)
				node := explorerNode{
					id:       nid,
					label:    p,
					depth:    0,
					isBranch: true,
					expanded: expandedSet[nid],
				}
				nodes = append(nodes, node)
				if node.expanded {
					if cached, ok := e.children[nid]; ok {
						nodes = e.appendExpandedChildren(nodes, nid, cached, expandedSet)
					}
				}
			}
		}
	}

	e.visible = nodes
	e.mu.Unlock()
	fyne.Do(func() { e.list.Refresh() })
}

// appendExpandedChildren recursively adds cached children (and their children) to the node list.
// Must be called with e.mu held.
func (e *Explorer) appendExpandedChildren(nodes []explorerNode, parentID string, childNodes []explorerNode, expandedSet map[string]bool) []explorerNode {
	for _, c := range childNodes {
		c.expanded = expandedSet[c.id]
		nodes = append(nodes, c)
		if c.expanded {
			if cached, ok := e.children[c.id]; ok {
				nodes = e.appendExpandedChildren(nodes, c.id, cached, expandedSet)
			}
		}
	}
	return nodes
}

// SetFavProjects updates the favorite projects list.
func (e *Explorer) SetFavProjects(projects []string) {
	e.mu.Lock()
	e.favProjects = projects
	e.mu.Unlock()
	e.rebuildVisible()
}

// SetRecentProjects updates the recent projects list.
func (e *Explorer) SetRecentProjects(projects []string) {
	e.mu.Lock()
	e.recentProjects = projects
	e.mu.Unlock()
	e.rebuildVisible()
}

// SetAllProjects sets the full project list from GCP.
func (e *Explorer) SetAllProjects(projects []string) {
	e.mu.Lock()
	e.allProjects = projects
	e.allLoaded = true
	e.allExpanded = true
	e.mu.Unlock()
	e.rebuildVisible()
}

// SetProjects is kept for backward compatibility — populates the "all" list.
func (e *Explorer) SetProjects(projects []string) {
	e.SetAllProjects(projects)
}

// AddProject adds a single project to the all list if not already present.
func (e *Explorer) AddProject(project string) {
	e.mu.Lock()
	for _, p := range e.allProjects {
		if p == project {
			e.mu.Unlock()
			return
		}
	}
	e.allProjects = append(e.allProjects, project)
	sort.Strings(e.allProjects)
	if !e.allLoaded {
		e.allLoaded = true
	}
	e.mu.Unlock()
	e.rebuildVisible()
}

// AllKnownProjects returns a deduplicated, sorted list of all known projects.
func (e *Explorer) AllKnownProjects() []string {
	e.mu.Lock()
	defer e.mu.Unlock()

	seen := make(map[string]bool)
	var result []string
	for _, p := range e.favProjects {
		if !seen[p] {
			seen[p] = true
			result = append(result, p)
		}
	}
	for _, p := range e.recentProjects {
		if !seen[p] {
			seen[p] = true
			result = append(result, p)
		}
	}
	for _, p := range e.allProjects {
		if !seen[p] {
			seen[p] = true
			result = append(result, p)
		}
	}
	sort.Strings(result)
	return result
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
			// Stop at headers too
			if e.visible[i].isHeader {
				break
			}
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
