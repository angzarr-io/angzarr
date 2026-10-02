package discovery

import (
	"encoding/json"
	"fmt"
	"strings"
)

// JSONSchema represents a JSON Schema definition.
type JSONSchema struct {
	Type        string                 `json:"type,omitempty"`
	Description string                 `json:"description,omitempty"`
	Properties  map[string]*JSONSchema `json:"properties,omitempty"`
	Items       *JSONSchema            `json:"items,omitempty"`
	Required    []string               `json:"required,omitempty"`
	Ref         string                 `json:"$ref,omitempty"`
	OneOf       []*JSONSchema          `json:"oneOf,omitempty"`
	Format      string                 `json:"format,omitempty"`
}

// DefinitionPrefix namespaces discovered types among the OpenAPI
// definitions generated for the framework API.
const DefinitionPrefix = "discovered."

// DefinitionNames maps each discovered type's full name to its OpenAPI
// definition name: `discovered.<Short>`, or `discovered.<full_name>` (dots
// as underscores) for every type whose short name is shared.
func DefinitionNames(types []DiscoveredType) map[string]string {
	count := make(map[string]int)
	for _, t := range types {
		count[shortName(t.FullName)]++
	}
	names := make(map[string]string, len(types))
	for _, t := range types {
		short := shortName(t.FullName)
		if count[short] > 1 {
			names[t.FullName] = DefinitionPrefix + strings.ReplaceAll(t.FullName, ".", "_")
		} else {
			names[t.FullName] = DefinitionPrefix + short
		}
	}
	return names
}

func shortName(fullName string) string {
	parts := strings.Split(fullName, ".")
	return parts[len(parts)-1]
}

// GenerateSchemas converts discovered types to JSON schemas keyed by
// definition name (see DefinitionNames); every `$ref` they contain points
// at one of those keys.
func GenerateSchemas(types []DiscoveredType) map[string]*JSONSchema {
	names := DefinitionNames(types)
	schemas := make(map[string]*JSONSchema, len(types))
	for _, t := range types {
		schemas[names[t.FullName]] = typeToSchema(t, names)
	}
	return schemas
}

func typeToSchema(t DiscoveredType, names map[string]string) *JSONSchema {
	schema := &JSONSchema{
		Type:        "object",
		Description: fmt.Sprintf("Proto message: %s", t.FullName),
		Properties:  make(map[string]*JSONSchema),
	}

	var required []string

	for _, f := range t.Fields {
		prop := fieldToSchema(f, names)
		schema.Properties[f.JSONName] = prop

		if !f.Optional && !f.Repeated {
			required = append(required, f.JSONName)
		}
	}

	if len(required) > 0 {
		schema.Required = required
	}

	return schema
}

func fieldToSchema(f FieldDef, names map[string]string) *JSONSchema {
	var base *JSONSchema
	if f.Enum {
		// Proto3 JSON renders enums as their value names.
		base = &JSONSchema{Type: "string", Description: fmt.Sprintf("Enum: %s", f.Type)}
	} else {
		base = primitiveSchema(f.Type, names)
	}

	if f.Repeated {
		return &JSONSchema{
			Type:  "array",
			Items: base,
		}
	}

	return base
}

func primitiveSchema(typeName string, names map[string]string) *JSONSchema {
	switch typeName {
	case "string":
		return &JSONSchema{Type: "string"}
	case "bytes":
		return &JSONSchema{Type: "string", Format: "byte"}
	case "bool":
		return &JSONSchema{Type: "boolean"}
	case "int32", "sint32", "sfixed32":
		return &JSONSchema{Type: "integer", Format: "int32"}
	case "int64", "sint64", "sfixed64":
		return &JSONSchema{Type: "string", Format: "int64"} // JSON doesn't handle 64-bit well
	case "uint32", "fixed32":
		return &JSONSchema{Type: "integer", Format: "int32"}
	case "uint64", "fixed64":
		return &JSONSchema{Type: "string", Format: "uint64"}
	case "float":
		return &JSONSchema{Type: "number", Format: "float"}
	case "double":
		return &JSONSchema{Type: "number", Format: "double"}
	case "google.protobuf.Timestamp":
		return &JSONSchema{Type: "string", Format: "date-time"}
	case "google.protobuf.Duration":
		return &JSONSchema{Type: "string", Format: "duration"}
	case "google.protobuf.Any":
		return &JSONSchema{
			Type:        "object",
			Description: "Any contains an arbitrary serialized protocol buffer message",
			Properties: map[string]*JSONSchema{
				"@type": {Type: "string", Description: "Type URL of the serialized message"},
			},
		}
	default:
		if name, ok := names[typeName]; ok {
			return &JSONSchema{Ref: "#/definitions/" + name}
		}
		// A message outside the discovered set (framework or
		// well-known type) has no definition to reference.
		return &JSONSchema{Type: "object", Description: fmt.Sprintf("Proto message: %s", typeName)}
	}
}

// BuildAnyOneOf creates a oneOf schema for google.protobuf.Any with discovered types.
func BuildAnyOneOf(types []DiscoveredType, filter func(DiscoveredType) bool) *JSONSchema {
	var oneOf []*JSONSchema
	names := DefinitionNames(types)

	for _, t := range types {
		if filter != nil && !filter(t) {
			continue
		}

		oneOf = append(oneOf, &JSONSchema{
			Type:        "object",
			Description: t.FullName,
			Properties: map[string]*JSONSchema{
				"@type": {
					Type:        "string",
					Description: fmt.Sprintf("Must be '%s'", t.TypeURL),
				},
			},
			Ref: "#/definitions/" + names[t.FullName],
		})
	}

	return &JSONSchema{
		OneOf:       oneOf,
		Description: "One of the discovered proto message types",
	}
}

// SchemasToJSON serializes schemas to JSON bytes.
func SchemasToJSON(schemas map[string]*JSONSchema) ([]byte, error) {
	return json.MarshalIndent(schemas, "", "  ")
}
