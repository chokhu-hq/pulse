package channel

import (
	"fmt"
	"strings"
	"sync"
)

type Channel struct {
	Name        string
	subscribers map[string]bool
	mux         sync.RWMutex
}

type Manager struct {
	channels map[string]*Channel
	mux      sync.RWMutex
}

func NewManager() *Manager {
	return &Manager{
		channels: make(map[string]*Channel),
	}
}

func (m *Manager) Subscribe(channelName, connID string) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	channel, exists := m.channels[channelName]
	if !exists {
		channel = &Channel{
			Name:        channelName,
			subscribers: make(map[string]bool),
		}
		m.channels[channelName] = channel
	}

	channel.mux.Lock()
	channel.subscribers[connID] = true
	channel.mux.Unlock()

	return nil
}

func (m *Manager) Unsubscribe(channelName, connID string) {
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

func (m *Manager) GetSubscribers(channelName string) []string {
	m.mux.RLock()
	channel, exists := m.channels[channelName]
	m.mux.RUnlock()

	if !exists {
		return []string{}
	}

	channel.mux.RLock()
	defer channel.mux.RUnlock()

	subscribers := make([]string, 0, len(channel.subscribers))
	for connID := range channel.subscribers {
		subscribers = append(subscribers, connID)
	}

	return subscribers
}

func (m *Manager) GetChannel(channelName string) (*Channel, error) {
	m.mux.RLock()
	defer m.mux.RUnlock()

	channel, exists := m.channels[channelName]
	if !exists {
		return nil, fmt.Errorf("channel not found: %s", channelName)
	}

	return channel, nil
}

func (m *Manager) GetChannelCount() int {
	m.mux.RLock()
	defer m.mux.RUnlock()
	return len(m.channels)
}

func (m *Manager) GetChannelNames(prefix string) []string {
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

func (m *Manager) GetSubscriberCount(channelName string) int {
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
