package discovery

import (
	"log"
	"sync"
)

// Service manages type discovery and OpenAPI patching.
type Service struct {
	baseSpec []byte

	mu          sync.RWMutex
	patchedSpec []byte
	types       []DiscoveredType
	collisions  CollisionReport
}

// NewService creates a discovery service from a descriptor file.
// If descriptorPath is empty, reads the DESCRIPTOR_PATH env var.
func NewService(baseSpec []byte, descriptorPath string) (*Service, error) {
	s := &Service{
		baseSpec:    baseSpec,
		patchedSpec: baseSpec,
	}

	// Load types from descriptor file
	var types []DiscoveredType
	var err error

	if descriptorPath != "" {
		types, err = LoadTypesFromDescriptorFile(descriptorPath)
	} else {
		types, err = LoadTypesFromDescriptorEnv()
	}

	if err != nil {
		return nil, err
	}

	if types == nil || len(types) == 0 {
		log.Printf("discovery: no descriptor file configured, OpenAPI spec unchanged")
		return s, nil
	}

	// Check for collisions
	s.collisions = DetectCollisions(types)
	if s.collisions.HasCollisions {
		log.Printf("discovery: WARNING - type name collisions detected:")
		for shortName, fullNames := range s.collisions.Collisions {
			log.Printf("  %s: %v", shortName, fullNames)
		}
	}

	// Patch OpenAPI spec
	patched, err := PatchOpenAPISpec(baseSpec, types)
	if err != nil {
		return nil, err
	}

	s.types = types
	s.patchedSpec = patched

	log.Printf("discovery: loaded %d types (%d events, %d commands)",
		len(types),
		countTypes(types, func(t DiscoveredType) bool { return t.IsEvent }),
		countTypes(types, func(t DiscoveredType) bool { return t.IsCommand }))

	return s, nil
}

func countTypes(types []DiscoveredType, pred func(DiscoveredType) bool) int {
	count := 0
	for _, t := range types {
		if pred(t) {
			count++
		}
	}
	return count
}

// GetSpec returns the patched OpenAPI spec.
func (s *Service) GetSpec() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.patchedSpec
}

// GetTypes returns the discovered types.
func (s *Service) GetTypes() []DiscoveredType {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.types
}

// GetCollisions returns any detected type name collisions.
func (s *Service) GetCollisions() CollisionReport {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.collisions
}

// GetInfo returns discovery status information.
func (s *Service) GetInfo() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	info := map[string]interface{}{
		"type_count":     len(s.types),
		"has_collisions": s.collisions.HasCollisions,
	}

	if s.collisions.HasCollisions {
		info["collisions"] = s.collisions.Collisions
	}

	eventCount := 0
	commandCount := 0
	for _, t := range s.types {
		if t.IsEvent {
			eventCount++
		}
		if t.IsCommand {
			commandCount++
		}
	}
	info["event_count"] = eventCount
	info["command_count"] = commandCount

	return info
}
