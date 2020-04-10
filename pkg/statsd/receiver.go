package statsd

import (
	"context"
	"net"
	"strings"
	"sync/atomic"

	"github.com/atlassian/gostatsd"
	"github.com/atlassian/gostatsd/pkg/fakesocket"
	"github.com/atlassian/gostatsd/pkg/pool"
	"github.com/atlassian/gostatsd/pkg/stats"

	"github.com/ash2k/stager/wait"
	"github.com/sirupsen/logrus"
)

// ip packet size is stored in two bytes and that is how big in theory the packet can be.
// In practice it is highly unlikely but still possible to get packets bigger than usual MTU of 1500.
const packetSizeUDP = 0xffff

// DatagramReceiver receives datagrams on its PacketConn and passes them off to be parsed
type DatagramReceiver struct {
	// Counter fields below must be read/written only using atomic instructions.
	// 64-bit fields must be the first fields in the struct to guarantee proper memory alignment.
	// See https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	datagramsReceived      uint64
	batchesRead            uint64
	cumulDatagramsReceived uint64

	bufPool *pool.DatagramBufferPool

	receiveBatchSize int // The number of datagrams to read in each batch
	numReaders       int
	socketFactory    SocketFactory

	out chan<- []*Datagram // Output chan of read datagram batches
}

// NewDatagramReceiver initialises a new DatagramReceiver.
func NewDatagramReceiver(out chan<- []*Datagram, sf SocketFactory, numReaders, receiveBatchSize int) *DatagramReceiver {
	return &DatagramReceiver{
		out:              out,
		receiveBatchSize: receiveBatchSize,
		numReaders:       numReaders,
		socketFactory:    sf,
		bufPool:          pool.NewDatagramBufferPool(packetSizeUDP),
	}
}

func (dr *DatagramReceiver) RunMetricsContext(ctx context.Context) {
	statser := stats.FromContext(ctx)
	flushed, unregister := statser.RegisterFlush()
	defer unregister()

	for {
		select {
		case <-ctx.Done():
			return
		case <-flushed:
			datagramsReceived := atomic.SwapUint64(&dr.datagramsReceived, 0)
			batchesRead := atomic.SwapUint64(&dr.batchesRead, 0)
			dr.cumulDatagramsReceived += datagramsReceived
			var avgDatagramsInBatch float64
			if batchesRead == 0 {
				avgDatagramsInBatch = 0
			} else {
				avgDatagramsInBatch = float64(datagramsReceived) / float64(batchesRead)
			}
			statser.Gauge("receiver.datagrams_received", float64(dr.cumulDatagramsReceived), nil)
			statser.Gauge("receiver.avg_datagrams_in_batch", avgDatagramsInBatch, nil)
		}
	}
}

func (dr *DatagramReceiver) Run(ctx context.Context) {
	wg := wait.Group{}
	var connections []net.PacketConn

	for r := 0; r < dr.numReaders; r++ {
		c, err := dr.socketFactory()
		if err != nil {
			logrus.WithError(err).Fatal("unable to create socket")
		}
		connections = append(connections, c)
		wg.StartWithContext(ctx, func(ctx context.Context) {
			dr.Receive(ctx, c)
		})
	}

	// Work until done
	<-ctx.Done()

	// Close all the sockets, which will make the receivers error out and stop
	for _, c := range connections {
		if e := c.Close(); e != nil && !strings.Contains(e.Error(), "use of closed network connection") {
			logrus.WithError(e).Warn("Error closing socket")
		}
	}

	// Wait for everything to stop
	wg.Wait()
}

// Receive accepts incoming datagrams on c, and passes them off to be parsed
func (dr *DatagramReceiver) Receive(ctx context.Context, c net.PacketConn) {
	br := NewBatchReader(c)
	messages := make([]Message, dr.receiveBatchSize)
	retBuffers := make([]*[][]byte, dr.receiveBatchSize)

	for i := 0; i < dr.receiveBatchSize; i++ {
		retBuffers[i] = dr.bufPool.Get()
		messages[i].Buffers = *retBuffers[i]
	}
	for {

		datagramCount, err := br.ReadBatch(messages)
		now := gostatsd.NanoNow()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if err != fakesocket.ErrClosedConnection && !strings.Contains(err.Error(), "use of closed network connection") {
				logrus.Warnf("Error reading from socket: %v", err)
			}
			continue
		}

		atomic.AddUint64(&dr.datagramsReceived, uint64(datagramCount))
		atomic.AddUint64(&dr.batchesRead, 1)

		dgs := make([]*Datagram, datagramCount)
		for i := 0; i < datagramCount; i++ {
			addr := messages[i].Addr
			nbytes := messages[i].N
			buf := messages[i].Buffers[0][:nbytes]

			retBuf := retBuffers[i]
			doneFn := func() {
				dr.bufPool.Put(retBuf)
			}

			dgs[i] = &Datagram{
				IP:        getIP(addr),
				Msg:       buf,
				Timestamp: now,
				DoneFunc:  doneFn,
			}
			retBuffers[i] = dr.bufPool.Get()
			messages[i].Buffers = *retBuffers[i]
		}
		select {
		case dr.out <- dgs:
			// success
		case <-ctx.Done():
			return
		}
	}
}

func getIP(addr net.Addr) gostatsd.IP {
	if a, ok := addr.(*net.UDPAddr); ok {
		return gostatsd.IP(a.IP.String())
	}
	logrus.Errorf("Cannot get source address %q of type %T", addr, addr)
	return gostatsd.UnknownIP
}
