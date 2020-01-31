package kubernetes

import (
	"context"
	"errors"
	"fmt"

	"k8s.io/client-go/tools/cache"
)

type LookupFunc func(key string) ([]interface{}, error)
type BlockingIndex struct {
	events           chan struct{}
	byKeyRequests    chan *byKeyRequest
	byKeyCancels     chan *byKeyRequest
	awaitingRequests map[*byKeyRequest]struct{}
	doLookup         LookupFunc
}

func NewBlockingGetByKey(informer cache.SharedIndexInformer) *BlockingIndex {
	return NewBlockingIndex(informer, byKeyLookup(informer))
}

func NewBlockingGetByIndex(informer cache.SharedIndexInformer, indexFunc cache.IndexFunc) (*BlockingIndex, error) {
	indexName := fmt.Sprintf("InformerNotifierIdx_%p", indexFunc)
	err := informer.AddIndexers(cache.Indexers{
		indexName: indexFunc,
	})
	if err != nil {
		return nil, err
	}
	return NewBlockingIndex(informer, byIndexLookup(informer, indexName)), nil
}

func NewBlockingIndex(informer cache.SharedIndexInformer, lookupFunc LookupFunc) *BlockingIndex {
	in := &BlockingIndex{
		events:           make(chan struct{}),
		byKeyRequests:    make(chan *byKeyRequest),
		byKeyCancels:     make(chan *byKeyRequest),
		awaitingRequests: make(map[*byKeyRequest]struct{}),
		doLookup:         lookupFunc,
	}
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    in.onAdd,
		UpdateFunc: in.onUpdate,
	})
	return in
}

func byIndexLookup(informer cache.SharedIndexInformer, indexName string) LookupFunc {
	byIndex := informer.GetIndexer().ByIndex
	return func(key string) ([]interface{}, error) {
		return byIndex(indexName, key)
	}
}

func byKeyLookup(informer cache.SharedIndexInformer) LookupFunc {
	getByKey := informer.GetIndexer().GetByKey
	return func(key string) ([]interface{}, error) {
		obj, exists, err := getByKey(key)
		if err != nil {
			return nil, err
		}
		if exists {
			return []interface{}{obj}, nil
		}
		return nil, nil
	}
}

func (b *BlockingIndex) Run(ctx context.Context) {
	defer b.unblockAwaitingRequests()
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.events:
			b.handleEvent()
		case request := <-b.byKeyRequests:
			b.handleRequest(request)
		case cancel := <-b.byKeyCancels:
			b.handleCancel(cancel)
		}
	}
}

func (b *BlockingIndex) Get(ctx context.Context, key string) ([]interface{}, error) {
	objs, err := b.doLookup(key)
	if err != nil {
		return nil, err
	}
	if len(objs) > 0 {
		return objs, nil
	}
	result := make(chan []interface{})
	resultErr := make(chan error)
	request := &byKeyRequest{
		key:       key,
		result:    result,
		resultErr: resultErr,
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case b.byKeyRequests <- request:
	}
	select {
	case <-ctx.Done():
		// Context signals done - we have three options now
		select {
		case b.byKeyCancels <- request:
			return nil, ctx.Err()
		case res := <-result:
			return res, nil
		case resErr := <-resultErr:
			return nil, resErr
		}
	case res := <-result:
		return res, nil
	case resErr := <-resultErr:
		return nil, resErr
	}
}

func (b *BlockingIndex) onAdd(obj interface{}) {
	b.events <- struct{}{}
}

func (b *BlockingIndex) onUpdate(oldObj, newObj interface{}) {
	b.events <- struct{}{}
}

func (b *BlockingIndex) unblockAwaitingRequests() {
	for request := range b.awaitingRequests {
		request.resultErr <- errors.New("blocking index terminated")
	}
}

func (b *BlockingIndex) handleEvent() {
	for request := range b.awaitingRequests {
		if b.tryToRespond(request) {
			delete(b.awaitingRequests, request)
		}
	}
}

func (b *BlockingIndex) handleCancel(cancel *byKeyRequest) {
	delete(b.awaitingRequests, cancel)
}

func (b *BlockingIndex) handleRequest(request *byKeyRequest) {
	if b.tryToRespond(request) {
		// Handled!
		return
	}
	// Put into the awaiting list
	b.awaitingRequests[request] = struct{}{}
}

func (b *BlockingIndex) tryToRespond(request *byKeyRequest) bool {
	objs, err := b.doLookup(request.key)
	if err != nil {
		request.resultErr <- err
		return true
	}
	if len(objs) > 0 {
		request.result <- objs
		return true
	}
	return false
}

type byKeyRequest struct {
	key       string
	result    chan<- []interface{}
	resultErr chan<- error
}
