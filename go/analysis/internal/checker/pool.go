package checker

import (
	"log"
	"sync"
)

type root struct {
	maxDepth int
	tree     map[int][]*action
	executed *sync.Map
}

func (r *root) exec() {
	for i := r.maxDepth; i >= 0; i-- {
		r.executeAt(i)
	}
}

func (r *root) executeAt(depth int) {
	acts := r.tree[depth]
	for _, act := range acts {
		r.execute(act)
	}
}

func (r *root) execute(act *action) {
	if _, ok := r.executed.LoadOrStore(act, struct{}{}); ok {
		return
	}

	if dbg('v') {
		log.Println("executing:", act.String())
	}

	act.execOnce()
}

func buildRoot(act *action, executed *sync.Map) *root {
	tree := make(map[int][]*action)

	tree[0] = append(tree[0], act)
	addActionsToTree(1, act.deps, tree)

	depth := 0
	for k := range tree {
		if k > depth {
			depth = k
		}
	}

	return &root{
		maxDepth: depth,
		tree:     tree,
		executed: executed,
	}
}

func addActionsToTree(depth int, actions []*action, tree map[int][]*action) {
	for _, act := range actions {
		tree[depth] = append(tree[depth], act)
		addActionsToTree(depth+1, act.deps, tree)
	}
}

// smartExecPool is a smart parallel executor for analysis passes. It takes the
// tree roots, builds a dependency graph and tries to execute each root in
// parallel using a pre-defined number of workers. In contrast to scheduling
// all goroutines like the stock `execAll` implementation does, this
// implementation tries to reduce memory usage by scheduling the passes smartly.
// Each worker executes the dependencies with the highest depth first
//
// This is a naive implementation and may deadlock when there are horizontal
// dependencies among horizontal passes.
type smartExecPool struct {
	getRoot  chan *root
	workers  int
	init     sync.Once
	done     sync.Once
	wg       sync.WaitGroup
	executed *sync.Map
}

func newSmartExecPool(workers int, roots []*action) *smartExecPool {
	numRoots := len(roots)

	pool := &smartExecPool{
		workers:  workers,
		getRoot:  make(chan *root, numRoots),
		executed: new(sync.Map),
	}
	pool.wg.Add(numRoots)

	if dbg('v') {
		log.Println("spawning a smart executor pool with", workers, "workers and", numRoots, "roots")
	}

	for _, act := range roots {
		pool.getRoot <- buildRoot(act, pool.executed)
	}

	return pool
}

func (s *smartExecPool) SpawnWorkers() {
	s.init.Do(func() {
		for i := 0; i < s.workers; i++ {
			go func() {
				for root := range s.getRoot {
					root.exec()
					s.wg.Done()
				}
			}()
		}
	})
}

func (s *smartExecPool) WaitAndDispose() {
	s.done.Do(func() {
		s.wg.Wait()
		close(s.getRoot)
		if dbg('v') {
			log.Println("smart executor pool disposed")
		}
	})
}
