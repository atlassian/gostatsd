package web

import (
	"net/http"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sync"
	"time"
)

type traceProfiler struct {
	mutex sync.Mutex
}

func (tp *traceProfiler) Trace(w http.ResponseWriter, r *http.Request) {
	tp.mutex.Lock()
	defer tp.mutex.Unlock()
	trace.Start(w)
	defer trace.Stop()
	time.Sleep(30 * time.Second)
}

func (tp *traceProfiler) PProf(w http.ResponseWriter, r *http.Request) {
	tp.mutex.Lock()
	defer tp.mutex.Unlock()
	pprof.StartCPUProfile(w)
	defer pprof.StopCPUProfile()
	time.Sleep(30 * time.Second)
}

func (tp *traceProfiler) MemProf(w http.ResponseWriter, r *http.Request) {
	tp.mutex.Lock()
	defer tp.mutex.Unlock()
	runtime.GC()
	pprof.Lookup("heap").WriteTo(w, 0)
}
