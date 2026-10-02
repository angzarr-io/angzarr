package discovery

import (
	"encoding/json"
	"fmt"
)

// OpenAPISpec represents a minimal OpenAPI v2 (Swagger) spec for patching.
type OpenAPISpec struct {
	Swagger     string                 `json:"swagger"`
	Info        map[string]interface{} `json:"info"`
	Host        string                 `json:"host,omitempty"`
	BasePath    string                 `json:"basePath,omitempty"`
	Schemes     []string               `json:"schemes,omitempty"`
	Consumes    []string               `json:"consumes,omitempty"`
	Produces    []string               `json:"produces,omitempty"`
	Paths       map[string]interface{} `json:"paths"`
	Definitions map[string]interface{} `json:"definitions"`
	Extra       map[string]interface{} `json:"-"` // Catch-all for other fields
}

// PatchOpenAPISpec adds discovered types to an existing OpenAPI spec.
func PatchOpenAPISpec(specBytes []byte, types []DiscoveredType) ([]byte, error) {
	// Parse existing spec
	var raw map[string]interface{}
	if err := json.Unmarshal(specBytes, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse OpenAPI spec: %w", err)
	}

	// Ensure definitions exists
	definitions, ok := raw["definitions"].(map[string]interface{})
	if !ok {
		definitions = make(map[string]interface{})
		raw["definitions"] = definitions
	}

	// Generate schemas for discovered types
	schemas := GenerateSchemas(types)

	// Add discovered schemas to definitions (names carry DefinitionPrefix)
	for name, schema := range schemas {
		// Convert JSONSchema to map for embedding
		schemaBytes, err := json.Marshal(schema)
		if err != nil {
			continue
		}
		var schemaMap map[string]interface{}
		if err := json.Unmarshal(schemaBytes, &schemaMap); err != nil {
			continue
		}

		definitions[name] = schemaMap
	}

	// Add composite types for events and commands
	eventTypes := filterTypes(types, func(t DiscoveredType) bool { return t.IsEvent })
	commandTypes := filterTypes(types, func(t DiscoveredType) bool { return t.IsCommand })

	if len(eventTypes) > 0 {
		definitions["discovered.AnyEvent"] = buildOneOfDefinition(eventTypes, "Event types discovered from the descriptor file")
	}
	if len(commandTypes) > 0 {
		definitions["discovered.AnyCommand"] = buildOneOfDefinition(commandTypes, "Command types discovered from the descriptor file")
	}

	// Add metadata about discovery
	if info, ok := raw["info"].(map[string]interface{}); ok {
		desc, _ := info["description"].(string)
		info["description"] = desc + "\n\n**Note:** This spec includes types discovered from the configured proto descriptor file (DESCRIPTOR_PATH)."
	}

	return json.MarshalIndent(raw, "", "  ")
}

func filterTypes(types []DiscoveredType, pred func(DiscoveredType) bool) []DiscoveredType {
	var result []DiscoveredType
	for _, t := range types {
		if pred(t) {
			result = append(result, t)
		}
	}
	return result
}

func buildOneOfDefinition(types []DiscoveredType, description string) map[string]interface{} {
	var oneOf []map[string]interface{}

	for _, t := range types {
		oneOf = append(oneOf, map[string]interface{}{
			"type":        "object",
			"description": t.FullName,
			"properties": map[string]interface{}{
				"@type": map[string]interface{}{
					"type":        "string",
					"description": fmt.Sprintf("Type URL: %s", t.TypeURL),
					"enum":        []string{t.TypeURL},
				},
			},
			"additionalProperties": true,
		})
	}

	return map[string]interface{}{
		"description": description,
		"oneOf":       oneOf,
	}
}

// DiscoveryInfo contains metadata about the discovery results.
type DiscoveryInfo struct {
	TypeCount    int      `json:"type_count"`
	EventCount   int      `json:"event_count"`
	CommandCount int      `json:"command_count"`
	TypeURLs     []string `json:"type_urls"`
}

// GetDiscoveryInfo returns summary information about discovered types.
func GetDiscoveryInfo(types []DiscoveredType) DiscoveryInfo {
	info := DiscoveryInfo{
		TypeCount: len(types),
	}

	for _, t := range types {
		info.TypeURLs = append(info.TypeURLs, t.TypeURL)
		if t.IsEvent {
			info.EventCount++
		}
		if t.IsCommand {
			info.CommandCount++
		}
	}

	return info
}
