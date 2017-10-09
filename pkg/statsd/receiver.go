package statsd

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/statser"

	log "github.com/sirupsen/logrus"
)

// ip packet size is stored in two bytes and that is how big in theory the packet can be.
// In practice it is highly unlikely but still possible to get packets bigger than usual MTU of 1500.
const packetSizeUDP = 0xffff

// DatagramReceiver receives datagrams on its PacketConn and passes them off to be parsed
type DatagramReceiver struct {
	// Counter fields below must be read/written only using atomic instructions.
	// 64-bit fields must be the first fields in the struct to guarantee proper memory alignment.
	// See https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	lastPacket      int64 // When last packet was received. Unix timestamp in nsec.
	packetsReceived uint64
	batchesRead     uint64

	statser          statser.Statser
	receiveBatchSize int // The number of packets to read in each batch

	out chan<- []*Datagram // Output chan of read datagram batches
}

// NewDatagramReceiver initialises a new DatagramReceiver.
func NewDatagramReceiver(out chan<- []*Datagram, receiveBatchSize int, statser statser.Statser) *DatagramReceiver {
	return &DatagramReceiver{
		out:              out,
		receiveBatchSize: receiveBatchSize,
		statser:          statser,
	}
}

func (dr *DatagramReceiver) RunMetrics(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second) // TODO: Make configurable
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			packetsReceived := float64(atomic.SwapUint64(&dr.packetsReceived, 0))
			batchesRead := atomic.SwapUint64(&dr.batchesRead, 0)
			var avgPacketsInBatch float64
			if batchesRead == 0 {
				avgPacketsInBatch = 0
			} else {
				avgPacketsInBatch = packetsReceived / float64(batchesRead)
			}
			dr.statser.Count("packets_received", packetsReceived, nil)
			dr.statser.Gauge("avg_packets_in_batch", avgPacketsInBatch, nil)
		}
	}
}

// Receive accepts incoming datagrams on c, and passes them off to be parsed
func (dr *DatagramReceiver) Receive(ctx context.Context, c net.PacketConn) {
	br := NewBatchReader(c)
	messages := make([]Message, dr.receiveBatchSize)
	bufPool := sync.Pool{
		New: func() interface{} {
			return make([]byte, packetSizeUDP)
		},
	}
	for {
		for i := 0; i < dr.receiveBatchSize; i++ {
			messages[i].Buffers = [][]byte{bufPool.Get().([]byte)}
		}
		packetCount, err := br.ReadBatch(messages)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			log.Warnf("Error reading from socket: %v", err)
			continue
		}
		// TODO consider updating counter for every N-th iteration to reduce contention
		atomic.AddUint64(&dr.packetsReceived, uint64(packetCount))
		atomic.StoreInt64(&dr.lastPacket, time.Now().UnixNano())
		atomic.AddUint64(&dr.batchesRead, 1)
		dgs := make([]*Datagram, packetCount)
		for i := 0; i < packetCount; i++ {
			addr := messages[i].Addr
			nbytes := messages[i].N
			buf := messages[i].Buffers[0]
			doneFn := func() {
				bufPool.Put(buf)
			}
			dgs[i] = &Datagram{
				IP:       getIP(addr),
				Msg:      buf[:nbytes],
				DoneFunc: doneFn,
			}
			select {
			case dr.out <- dgs:
			case <-ctx.Done():
				return
			}
		}
	}
}

func getIP(addr net.Addr) gostatsd.IP {
	if a, ok := addr.(*net.UDPAddr); ok {
		return gostatsd.IP(a.IP.String())
	}
	log.Errorf("Cannot get source address %q of type %T", addr, addr)
	return gostatsd.UnknownIP
}
