package channel

import (
	"fmt"
	"strings"
	"sync"
)

type Channel[T any] struct {
	Name        string
	subscribers map[string]T
	mux         sync.RWMutex
}

type Manager[T any] struct {
	channels map[string]*Channel[T]
	mux      sync.RWMutex
}

func NewManager[T any]() *Manager[T] {
	return &Manager[T]{
		channels: make(map[string]*Channel[T]),
	}
}

func (m *Manager[T]) Subscribe(channelName, connID string, sub T) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	channel, exists := m.channels[channelName]
	if !exists {
		channel = &Channel[T]{
			Name:        channelName,
			subscribers: make(map[string]T),
		}
		m.channels[channelName] = channel
	}

	channel.mux.Lock()
	channel.subscribers[connID] = sub
	channel.mux.Unlock()

	return nil
}

func (m *Manager[T]) Unsubscribe(channelName, connID string) {
	m.mux.RLock()
	channel, exists := m.channels[channelName]
	m.mux.RUnlock()

	if !exists {
		return
	}

	channel.mux.Lock()
	delete(channel.subscribers, connID)
	isEmpty := len(channel.subscribers) == 0
	channel.mux.Unlock()

	if isEmpty {
		m.mux.Lock()
		if ch, exists := m.channels[channelName]; exists {
			ch.mux.Lock()
			if len(ch.subscribers) == 0 {
				delete(m.channels, channelName)
			}
			ch.mux.Unlock()
		}
		m.mux.Unlock()
	}
}

// fn runs under the channel read lock, so it must not block or call back into the manager
func (m *Manager[T]) ForEachSubscriber(channelName string, fn func(connID string, sub T)) {
	m.mux.RLock()
	channel, exists := m.channels[channelName]
	m.mux.RUnlock()

	if !exists {
		return
	}

	channel.mux.RLock()
	defer channel.mux.RUnlock()

	for connID, sub := range channel.subscribers {
		fn(connID, sub)
	}
}

func (m *Manager[T]) GetChannel(channelName string) (*Channel[T], error) {
	m.mux.RLock()
	defer m.mux.RUnlock()

	channel, exists := m.channels[channelName]
	if !exists {
		return nil, fmt.Errorf("channel not found: %s", channelName)
	}

	return channel, nil
}

func (m *Manager[T]) GetChannelCount() int {
	m.mux.RLock()
	defer m.mux.RUnlock()
	return len(m.channels)
}

func (m *Manager[T]) GetChannelNames(prefix string) []string {
	m.mux.RLock()
	defer m.mux.RUnlock()

	names := make([]string, 0)
	for name := range m.channels {
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
	return names
}

func (m *Manager[T]) GetSubscriberCount(channelName string) int {
	m.mux.RLock()
	channel, exists := m.channels[channelName]
	m.mux.RUnlock()

	if !exists {
		return 0
	}

	channel.mux.RLock()
	defer channel.mux.RUnlock()
	return len(channel.subscribers)
}
