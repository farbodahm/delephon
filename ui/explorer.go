package ui

import (
	"fmt"
	"image/color"
	"log"
	"sort"
	"strings"
	"sync"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/canvas"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
)

// Node ID format:
//   "p:<project>"
//   "d:<project>/<dataset>"
//   "t:<project>/<dataset>/<table>"

func ProjectNodeID(project string) string          { return "p:" + project }
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
	depth    int // 0=project/header, 1=dataset, 2=table
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

	searchFilter     string          // current search text
	searchInProgress map[string]bool // projects currently being loaded for search

	LoadChildren      LoadChildrenFunc
	OnTableSelected   OnTableSelectedFunc
	OnLoadAllProjects func()               // callback to load all projects from GCP
	OnProjectSelected func(project string) // callback when a project node is clicked (set in editor)
	OnSearchProject   func(project string) // callback: load all datasets+tables for a project

	Container fyne.CanvasObject
}

func NewExplorer() *Explorer {
	e := &Explorer{
		children:         make(map[string][]explorerNode),
		loading:          make(map[string]bool),
		searchInProgress: make(map[string]bool),
		favExpanded:      true,
		recentExpanded:   true,
		allExpanded:      false,
	}

	e.searchEntry = widget.NewEntry()
	e.searchEntry.SetPlaceHolder("Filter projects & tables...")
	e.searchEntry.OnChanged = func(text string) {
		e.mu.Lock()
		e.searchFilter = text
		e.mu.Unlock()
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
			label := canvas.NewText("template", color.White)
			leftGroup := container.NewHBox(spacer, icon)
			return container.NewBorder(nil, nil, leftGroup, nil, label)
		},
		func(id widget.ListItemID, obj fyne.CanvasObject) {
			e.mu.Lock()
			if id >= len(e.visible) {
				e.mu.Unlock()
				return
			}
			node := e.visible[id]
			e.mu.Unlock()

			c := obj.(*fyne.Container)
			label := c.Objects[0].(*canvas.Text)
			leftGroup := c.Objects[1].(*fyne.Container)
			spacer := leftGroup.Objects[0].(*widget.Label)
			icon := leftGroup.Objects[1].(*widget.Icon)

			// Indentation
			indent := ""
			for i := 0; i < node.depth; i++ {
				indent += "    "
			}
			spacer.SetText(indent)

			label.Text = node.label
			label.Color = explorerNodeColor(node)
			label.TextSize = theme.Size(theme.SizeNameText)

			if node.isHeader {
				label.TextStyle = fyne.TextStyle{Bold: true}
				if node.expanded {
					icon.SetResource(theme.MoveDownIcon())
				} else {
					icon.SetResource(theme.NavigateNextIcon())
				}
				label.Refresh()
				return
			}

			label.TextStyle = fyne.TextStyle{}

			if node.isBranch {
				if node.expanded {
					icon.SetResource(theme.MoveDownIcon())
				} else {
					icon.SetResource(theme.NavigateNextIcon())
				}
			} else {
				icon.SetResource(theme.DocumentIcon())
			}
			label.Refresh()
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
		// Search mode: only fav + recent projects, matching project names and table names
		seen := make(map[string]bool)

		type projectMatch struct {
			name         string
			nameMatch    bool
			tableMatches []explorerNode
		}
		var tableMatchProjects []projectMatch
		var nameOnlyProjects []projectMatch
		var toLoad []string

		for _, list := range [][]string{e.favProjects, e.recentProjects} {
			for _, p := range list {
				if seen[p] {
					continue
				}
				seen[p] = true

				nameMatch := strings.Contains(strings.ToLower(p), filter)
				tableMatches := e.cachedTableMatchesLocked(p, filter)

				if !nameMatch && len(tableMatches) == 0 {
					// No cached children yet — trigger background load
					pid := ProjectNodeID(p)
					if _, hasCached := e.children[pid]; !hasCached && !e.searchInProgress[p] {
						e.searchInProgress[p] = true
						toLoad = append(toLoad, p)
					}
					continue
				}

				pm := projectMatch{name: p, nameMatch: nameMatch, tableMatches: tableMatches}
				if len(tableMatches) > 0 {
					tableMatchProjects = append(tableMatchProjects, pm)
				} else {
					nameOnlyProjects = append(nameOnlyProjects, pm)
				}
			}
		}

		// Projects with table matches first, then name-only matches
		for _, pm := range tableMatchProjects {
			nid := ProjectNodeID(pm.name)
			nodes = append(nodes, explorerNode{
				id:       nid,
				label:    pm.name,
				depth:    0,
				isBranch: true,
				expanded: true,
			})
			for _, tbl := range pm.tableMatches {
				nodes = append(nodes, tbl)
			}
		}
		for _, pm := range nameOnlyProjects {
			nid := ProjectNodeID(pm.name)
			node := explorerNode{
				id:       nid,
				label:    pm.name,
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

		// Trigger background loading for uncached projects (outside lock)
		e.mu.Unlock()
		if e.OnSearchProject != nil {
			for _, p := range toLoad {
				p := p
				go e.OnSearchProject(p)
			}
		}
		e.mu.Lock()
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

// CacheProjectData is called after parallel BQ loading completes.
// It populates children caches for the project's datasets and tables,
// clears searchInProgress, and triggers rebuildVisible.
func (e *Explorer) CacheProjectData(project string, datasets map[string][]string) {
	e.mu.Lock()
	pid := ProjectNodeID(project)

	// Build dataset child nodes for the project
	var dsNames []string
	for ds := range datasets {
		dsNames = append(dsNames, ds)
	}
	sort.Strings(dsNames)

	dsNodes := make([]explorerNode, len(dsNames))
	for i, ds := range dsNames {
		did := DatasetNodeID(project, ds)
		dsNodes[i] = explorerNode{
			id:       did,
			label:    ds,
			depth:    1,
			isBranch: true,
		}

		// Build table child nodes for each dataset
		tables := datasets[ds]
		tblNodes := make([]explorerNode, len(tables))
		for j, tbl := range tables {
			tblNodes[j] = explorerNode{
				id:    TableNodeID(project, ds, tbl),
				label: tbl,
				depth: 2,
			}
		}
		e.children[did] = tblNodes
	}
	e.children[pid] = dsNodes

	delete(e.searchInProgress, project)
	e.mu.Unlock()

	e.rebuildVisible()
}

// cachedTableMatchesLocked walks cached datasets and tables for a project
// and returns table nodes whose label (formatted as "dataset.table") contains the filter.
// Must be called with e.mu held.
func (e *Explorer) cachedTableMatchesLocked(project, filter string) []explorerNode {
	pid := ProjectNodeID(project)
	dsNodes, ok := e.children[pid]
	if !ok {
		return nil
	}
	var matches []explorerNode
	for _, dsNode := range dsNodes {
		tblNodes, ok := e.children[dsNode.id]
		if !ok {
			continue
		}
		_, _, dataset, _ := ParseNodeID(dsNode.id)
		for _, tblNode := range tblNodes {
			fqLabel := dataset + "." + tblNode.label
			if strings.Contains(strings.ToLower(fqLabel), filter) {
				matches = append(matches, explorerNode{
					id:    tblNode.id,
					label: fqLabel,
					depth: 1,
				})
			}
		}
	}
	return matches
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

func explorerNodeColor(node explorerNode) color.Color {
	t := fyne.CurrentApp().Settings().Theme()
	if node.isHeader {
		return t.Color("explorerHeader", 0)
	}
	switch node.depth {
	case 1: // dataset
		return t.Color("explorerDataset", 0)
	case 2: // table
		return t.Color("explorerTable", 0)
	default: // project
		return t.Color("explorerProject", 0)
	}
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
