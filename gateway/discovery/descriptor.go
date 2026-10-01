package discovery

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// LoadDescriptorSet loads a FileDescriptorSet from a binary file (buf build output).
func LoadDescriptorSet(path string) (*descriptorpb.FileDescriptorSet, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read descriptor set: %w", err)
	}

	var fds descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(data, &fds); err != nil {
		return nil, fmt.Errorf("failed to parse descriptor set: %w", err)
	}

	return &fds, nil
}

// ExtractTypesFromDescriptorSet extracts all message types from a FileDescriptorSet.
func ExtractTypesFromDescriptorSet(fds *descriptorpb.FileDescriptorSet) ([]DiscoveredType, error) {
	registry, err := protodesc.NewFiles(fds)
	if err != nil {
		return nil, fmt.Errorf("failed to create file registry: %w", err)
	}

	var types []DiscoveredType

	registry.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		// Skip well-known types and framework packages
		pkg := string(fd.Package())
		if shouldSkipPackage(pkg) {
			return true
		}

		msgs := fd.Messages()
		for i := 0; i < msgs.Len(); i++ {
			msg := msgs.Get(i)
			types = append(types, extractMessageTypes(msg)...)
		}
		return true
	})

	// Sort by full name for stable output
	sort.Slice(types, func(i, j int) bool {
		return types[i].FullName < types[j].FullName
	})

	return types, nil
}

// LoadTypesFromDescriptorFile loads types from a buf build output file.
func LoadTypesFromDescriptorFile(path string) ([]DiscoveredType, error) {
	fds, err := LoadDescriptorSet(path)
	if err != nil {
		return nil, err
	}
	return ExtractTypesFromDescriptorSet(fds)
}

// DescriptorPathEnvVar is the environment variable for the descriptor file path.
// Shared with angzarr core for consistency.
const DescriptorPathEnvVar = "DESCRIPTOR_PATH"

// LoadTypesFromDescriptorEnv loads types from DESCRIPTOR_PATH env var.
func LoadTypesFromDescriptorEnv() ([]DiscoveredType, error) {
	path := os.Getenv(DescriptorPathEnvVar)
	if path == "" {
		return nil, nil
	}
	return LoadTypesFromDescriptorFile(path)
}

func shouldSkipPackage(pkg string) bool {
	skipPrefixes := []string{
		"google.",
		"grpc.",
		"buf.",
		"angzarr.",    // Framework types (legacy package), only want business types
		"io.angzarr.", // Framework types (io.angzarr.v1, io.angzarr.status.v1)
	}
	for _, prefix := range skipPrefixes {
		if strings.HasPrefix(pkg, prefix) {
			return true
		}
	}
	return false
}

func extractMessageTypes(msg protoreflect.MessageDescriptor) []DiscoveredType {
	var types []DiscoveredType

	fullName := string(msg.FullName())
	simpleName := string(msg.Name())

	dt := DiscoveredType{
		FullName:  fullName,
		TypeURL:   "type.googleapis.com/" + fullName,
		IsEvent:   isEventName(simpleName),
		IsCommand: isCommandName(simpleName),
	}

	// Extract fields
	fields := msg.Fields()
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		dt.Fields = append(dt.Fields, FieldDef{
			Name:     string(f.Name()),
			JSONName: f.JSONName(),
			Type:     fieldTypeString(f),
			Repeated: f.Cardinality() == protoreflect.Repeated && !f.IsMap(),
			Optional: f.HasOptionalKeyword(),
			Enum:     f.Kind() == protoreflect.EnumKind,
		})
	}

	types = append(types, dt)

	// Recurse into nested messages
	nested := msg.Messages()
	for i := 0; i < nested.Len(); i++ {
		types = append(types, extractMessageTypes(nested.Get(i))...)
	}

	return types
}

func fieldTypeString(f protoreflect.FieldDescriptor) string {
	switch f.Kind() {
	case protoreflect.MessageKind:
		return string(f.Message().FullName())
	case protoreflect.EnumKind:
		return string(f.Enum().FullName())
	default:
		return f.Kind().String()
	}
}

// CollisionReport contains information about type name collisions.
type CollisionReport struct {
	// Collisions maps short name to list of full names that share it
	Collisions map[string][]string
	// HasCollisions is true if any collisions were found
	HasCollisions bool
}

// DetectCollisions finds type name collisions across all discovered types.
// Collisions occur when different packages define types with the same short name.
func DetectCollisions(types []DiscoveredType) CollisionReport {
	// Map short name -> list of full names
	shortToFull := make(map[string][]string)

	for _, t := range types {
		parts := strings.Split(t.FullName, ".")
		shortName := parts[len(parts)-1]
		shortToFull[shortName] = append(shortToFull[shortName], t.FullName)
	}

	report := CollisionReport{
		Collisions: make(map[string][]string),
	}

	for shortName, fullNames := range shortToFull {
		if len(fullNames) > 1 {
			report.Collisions[shortName] = fullNames
			report.HasCollisions = true
		}
	}

	return report
}

// MergeDescriptorSets combines multiple FileDescriptorSets into one.
// Useful for combining types from multiple domains.
func MergeDescriptorSets(sets ...*descriptorpb.FileDescriptorSet) *descriptorpb.FileDescriptorSet {
	merged := &descriptorpb.FileDescriptorSet{}
	seen := make(map[string]bool)

	for _, fds := range sets {
		for _, fd := range fds.File {
			if !seen[fd.GetName()] {
				seen[fd.GetName()] = true
				merged.File = append(merged.File, fd)
			}
		}
	}

	return merged
}
