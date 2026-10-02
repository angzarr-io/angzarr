package discovery

import (
	"encoding/json"
	"testing"
)

func TestPrimitiveSchema(t *testing.T) {
	tests := []struct {
		typeName   string
		wantType   string
		wantFormat string
		wantRef    string
	}{
		{"string", "string", "", ""},
		{"bytes", "string", "byte", ""},
		{"bool", "boolean", "", ""},
		{"int32", "integer", "int32", ""},
		{"sint32", "integer", "int32", ""},
		{"sfixed32", "integer", "int32", ""},
		{"int64", "string", "int64", ""},
		{"sint64", "string", "int64", ""},
		{"sfixed64", "string", "int64", ""},
		{"uint32", "integer", "int32", ""},
		{"fixed32", "integer", "int32", ""},
		{"uint64", "string", "uint64", ""},
		{"fixed64", "string", "uint64", ""},
		{"float", "number", "float", ""},
		{"double", "number", "double", ""},
		{"google.protobuf.Timestamp", "string", "date-time", ""},
		{"google.protobuf.Duration", "string", "duration", ""},
	}

	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			schema := primitiveSchema(tt.typeName, nil)
			if schema.Type != tt.wantType {
				t.Errorf("Type = %q, want %q", schema.Type, tt.wantType)
			}
			if schema.Format != tt.wantFormat {
				t.Errorf("Format = %q, want %q", schema.Format, tt.wantFormat)
			}
			if schema.Ref != tt.wantRef {
				t.Errorf("Ref = %q, want %q", schema.Ref, tt.wantRef)
			}
		})
	}
}

func TestPrimitiveSchema_Any(t *testing.T) {
	schema := primitiveSchema("google.protobuf.Any", nil)
	if schema.Type != "object" {
		t.Errorf("Any type = %q, want \"object\"", schema.Type)
	}
	if schema.Properties == nil {
		t.Fatal("Any should have properties")
	}
	atType, ok := schema.Properties["@type"]
	if !ok {
		t.Fatal("Any should have @type property")
	}
	if atType.Type != "string" {
		t.Errorf("@type type = %q, want \"string\"", atType.Type)
	}
}

func TestPrimitiveSchema_MessageRef(t *testing.T) {
	names := map[string]string{"examples.player.PlayerState": "discovered.PlayerState"}
	schema := primitiveSchema("examples.player.PlayerState", names)
	if schema.Ref != "#/definitions/discovered.PlayerState" {
		t.Errorf("Ref = %q, want \"#/definitions/discovered.PlayerState\"", schema.Ref)
	}
	if schema.Type != "" {
		t.Errorf("Type should be empty for refs, got %q", schema.Type)
	}
}

// A message outside the discovered set (e.g. a framework type) has no
// definition, so it must not produce a dangling $ref.
func TestPrimitiveSchema_UndiscoveredMessageIsInlineObject(t *testing.T) {
	schema := primitiveSchema("io.angzarr.v1.Cover", map[string]string{})
	if schema.Ref != "" {
		t.Errorf("undiscovered message produced dangling Ref %q", schema.Ref)
	}
	if schema.Type != "object" {
		t.Errorf("Type = %q, want object", schema.Type)
	}
}

func TestFieldToSchema_Scalar(t *testing.T) {
	f := FieldDef{Name: "id", JSONName: "id", Type: "string", Repeated: false}
	schema := fieldToSchema(f, nil)
	if schema.Type != "string" {
		t.Errorf("Type = %q, want \"string\"", schema.Type)
	}
	if schema.Items != nil {
		t.Error("scalar field should not have Items")
	}
}

func TestFieldToSchema_Repeated(t *testing.T) {
	f := FieldDef{Name: "tags", JSONName: "tags", Type: "string", Repeated: true}
	schema := fieldToSchema(f, nil)
	if schema.Type != "array" {
		t.Errorf("Type = %q, want \"array\"", schema.Type)
	}
	if schema.Items == nil {
		t.Fatal("repeated field should have Items")
	}
	if schema.Items.Type != "string" {
		t.Errorf("Items.Type = %q, want \"string\"", schema.Items.Type)
	}
}

func TestFieldToSchema_RepeatedMessage(t *testing.T) {
	f := FieldDef{Name: "items", JSONName: "items", Type: "examples.Order.Item", Repeated: true}
	schema := fieldToSchema(f, map[string]string{"examples.Order.Item": "discovered.Item"})
	if schema.Type != "array" {
		t.Errorf("Type = %q, want \"array\"", schema.Type)
	}
	if schema.Items == nil {
		t.Fatal("repeated field should have Items")
	}
	if schema.Items.Ref != "#/definitions/discovered.Item" {
		t.Errorf("Items.Ref = %q, want \"#/definitions/discovered.Item\"", schema.Items.Ref)
	}
}

// Proto3 JSON renders enums as value names; an enum field is a string, not
// a reference to a (non-existent) message definition.
func TestFieldToSchema_EnumIsString(t *testing.T) {
	f := FieldDef{Name: "status", JSONName: "status", Type: "examples.Status", Enum: true}
	schema := fieldToSchema(f, map[string]string{})
	if schema.Type != "string" || schema.Ref != "" {
		t.Errorf("enum schema = %+v, want string without ref", schema)
	}
}

func TestTypeToSchema(t *testing.T) {
	dt := DiscoveredType{
		FullName: "examples.player.PlayerRegistered",
		Fields: []FieldDef{
			{Name: "player_id", JSONName: "playerId", Type: "string", Repeated: false, Optional: false},
			{Name: "amount", JSONName: "amount", Type: "int64", Repeated: false, Optional: false},
			{Name: "tags", JSONName: "tags", Type: "string", Repeated: true, Optional: false},
			{Name: "nickname", JSONName: "nickname", Type: "string", Repeated: false, Optional: true},
		},
	}

	schema := typeToSchema(dt, nil)

	if schema.Type != "object" {
		t.Errorf("Type = %q, want \"object\"", schema.Type)
	}
	if schema.Description != "Proto message: examples.player.PlayerRegistered" {
		t.Errorf("unexpected Description: %s", schema.Description)
	}
	if len(schema.Properties) != 4 {
		t.Errorf("expected 4 properties, got %d", len(schema.Properties))
	}

	// Required should include non-optional, non-repeated fields
	// playerId (required), amount (required), tags (repeated—excluded), nickname (optional—excluded)
	if len(schema.Required) != 2 {
		t.Fatalf("expected 2 required fields, got %d: %v", len(schema.Required), schema.Required)
	}
	requiredSet := map[string]bool{}
	for _, r := range schema.Required {
		requiredSet[r] = true
	}
	if !requiredSet["playerId"] {
		t.Error("playerId should be required")
	}
	if !requiredSet["amount"] {
		t.Error("amount should be required")
	}
}

func TestTypeToSchema_NoRequired(t *testing.T) {
	dt := DiscoveredType{
		FullName: "examples.Empty",
		Fields: []FieldDef{
			{Name: "opt", JSONName: "opt", Type: "string", Optional: true},
			{Name: "rep", JSONName: "rep", Type: "string", Repeated: true},
		},
	}
	schema := typeToSchema(dt, nil)
	if schema.Required != nil {
		t.Errorf("expected nil Required, got %v", schema.Required)
	}
}

func TestTypeToSchema_NoFields(t *testing.T) {
	dt := DiscoveredType{FullName: "examples.Empty"}
	schema := typeToSchema(dt, nil)
	if schema.Type != "object" {
		t.Errorf("Type = %q, want \"object\"", schema.Type)
	}
	if len(schema.Properties) != 0 {
		t.Errorf("expected 0 properties, got %d", len(schema.Properties))
	}
	if schema.Required != nil {
		t.Errorf("expected nil Required, got %v", schema.Required)
	}
}

func TestGenerateSchemas_Basic(t *testing.T) {
	types := []DiscoveredType{
		{
			FullName: "examples.player.PlayerRegistered",
			Fields: []FieldDef{
				{Name: "id", JSONName: "id", Type: "string"},
			},
		},
		{
			FullName: "examples.order.OrderCreated",
			Fields: []FieldDef{
				{Name: "order_id", JSONName: "orderId", Type: "string"},
			},
		},
	}

	schemas := GenerateSchemas(types)

	if len(schemas) != 2 {
		t.Fatalf("expected 2 schemas, got %d", len(schemas))
	}
	if _, ok := schemas["discovered.PlayerRegistered"]; !ok {
		t.Error("missing discovered.PlayerRegistered schema")
	}
	if _, ok := schemas["discovered.OrderCreated"]; !ok {
		t.Error("missing discovered.OrderCreated schema")
	}
}

func TestGenerateSchemas_Collision(t *testing.T) {
	types := []DiscoveredType{
		{FullName: "pkg1.Event"},
		{FullName: "pkg2.Event"},
	}

	schemas := GenerateSchemas(types)

	// A shared short name is ambiguous, so every type sharing it is keyed
	// by its full name.
	if len(schemas) != 2 {
		t.Fatalf("expected 2 schemas, got %d", len(schemas))
	}
	for _, key := range []string{"discovered.pkg1_Event", "discovered.pkg2_Event"} {
		if _, ok := schemas[key]; !ok {
			t.Errorf("missing %s", key)
		}
	}
}

// Every $ref in the generated schemas names a generated definition.
func TestGenerateSchemas_RefsResolve(t *testing.T) {
	types := []DiscoveredType{
		{FullName: "pkg1.Event", Fields: []FieldDef{{Name: "item", JSONName: "item", Type: "pkg1.Item"}}},
		{FullName: "pkg2.Event", Fields: []FieldDef{{Name: "items", JSONName: "items", Type: "pkg1.Item", Repeated: true}}},
		{FullName: "pkg1.Item"},
	}
	schemas := GenerateSchemas(types)

	var refs []string
	for _, s := range schemas {
		for _, p := range s.Properties {
			if p.Ref != "" {
				refs = append(refs, p.Ref)
			}
			if p.Items != nil && p.Items.Ref != "" {
				refs = append(refs, p.Items.Ref)
			}
		}
	}
	if len(refs) != 2 {
		t.Fatalf("expected 2 refs, got %v", refs)
	}
	for _, ref := range refs {
		if _, ok := schemas[ref[len("#/definitions/"):]]; !ok {
			t.Errorf("dangling ref %s", ref)
		}
	}
}

func TestBuildAnyOneOf_NoFilter(t *testing.T) {
	types := []DiscoveredType{
		{FullName: "pkg.TypeA", TypeURL: "type.googleapis.com/pkg.TypeA"},
		{FullName: "pkg.TypeB", TypeURL: "type.googleapis.com/pkg.TypeB"},
	}

	schema := BuildAnyOneOf(types, nil)

	if len(schema.OneOf) != 2 {
		t.Fatalf("expected 2 oneOf entries, got %d", len(schema.OneOf))
	}
	if schema.Description != "One of the discovered proto message types" {
		t.Errorf("unexpected Description: %s", schema.Description)
	}

	// Verify first entry
	entry := schema.OneOf[0]
	if entry.Type != "object" {
		t.Errorf("entry Type = %q, want \"object\"", entry.Type)
	}
	if entry.Ref != "#/definitions/discovered.TypeA" {
		t.Errorf("entry Ref = %q, want \"#/definitions/discovered.TypeA\"", entry.Ref)
	}
}

func TestBuildAnyOneOf_WithFilter(t *testing.T) {
	types := []DiscoveredType{
		{FullName: "pkg.EventA", TypeURL: "type.googleapis.com/pkg.EventA", IsEvent: true},
		{FullName: "pkg.CommandB", TypeURL: "type.googleapis.com/pkg.CommandB", IsCommand: true},
		{FullName: "pkg.EventC", TypeURL: "type.googleapis.com/pkg.EventC", IsEvent: true},
	}

	schema := BuildAnyOneOf(types, func(dt DiscoveredType) bool { return dt.IsEvent })

	if len(schema.OneOf) != 2 {
		t.Fatalf("expected 2 oneOf entries (events only), got %d", len(schema.OneOf))
	}
}

func TestBuildAnyOneOf_EmptyTypes(t *testing.T) {
	schema := BuildAnyOneOf(nil, nil)
	if schema.OneOf != nil {
		t.Errorf("expected nil oneOf for empty types, got %d entries", len(schema.OneOf))
	}
}

func TestSchemasToJSON(t *testing.T) {
	schemas := map[string]*JSONSchema{
		"Test": {Type: "object", Description: "test schema"},
	}

	data, err := SchemasToJSON(schemas)
	if err != nil {
		t.Fatalf("SchemasToJSON error: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}

	testSchema, ok := parsed["Test"].(map[string]interface{})
	if !ok {
		t.Fatal("missing Test schema in output")
	}
	if testSchema["type"] != "object" {
		t.Errorf("type = %v, want \"object\"", testSchema["type"])
	}
}

func TestSchemasToJSON_Empty(t *testing.T) {
	schemas := map[string]*JSONSchema{}
	data, err := SchemasToJSON(schemas)
	if err != nil {
		t.Fatalf("SchemasToJSON error: %v", err)
	}
	if string(data) != "{}" {
		t.Errorf("expected empty JSON object, got %s", string(data))
	}
}
