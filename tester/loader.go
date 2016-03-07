package main

import (
	"math/rand"
	"sync/atomic"
	"time"
)

func (s *Server) load() {
	for {
		select {
		case <-s.start:
			atomic.StoreInt64(&s.Stats.NumMetrics, 0)
			atomic.StoreInt64(&s.Stats.NumPackets, 0)
			s.Stats.StartTime = time.Now()
			s.Started = true

			go func() {
				for worker := 0; worker < s.Concurrency; worker++ {
					rand.Seed(time.Now().Unix())
					go func() {
						for s.Started {
							buf := s.writeLines()
							if buf.Len() > 0 {
								s.write(buf)
							}
							time.Sleep(100 * time.Microsecond)
						}
					}()
				}
			}()
		case <-s.stop:
			s.Started = false
			s.gatherStats()
		}
	}
}
