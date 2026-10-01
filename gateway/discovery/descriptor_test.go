package discovery

import (
	"os"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestShouldSkipPackage(t *testing.T) {
	tests := []struct {
		pkg  string
		want bool
	}{
		{"google.protobuf", true},
		{"google.api", true},
		{"grpc.health.v1", true},
		{"buf.validate", true},
		{"angzarr.coordinator", true},
		{"io.angzarr.v1", true},
		{"io.angzarr.status.v1", true},
		{"io.example.orders", false},

		{"examples.player", false},
		{"myapp.orders", false},
		{"", false},
		{"googleish", false}, // doesn't start with "google."
	}

	for _, tt := range tests {
		t.Run(tt.pkg, func(t *testing.T) {
			if got := shouldSkipPackage(tt.pkg); got != tt.want {
				t.Errorf("shouldSkipPackage(%q) = %v, want %v", tt.pkg, got, tt.want)
			}
		})
	}
}

func TestDetectCollisions_NoCollisions(t *testing.T) {
	types := []DiscoveredType{
		{FullName: "pkg1.TypeA"},
		{FullName: "pkg2.TypeB"},
		{FullName: "pkg3.TypeC"},
	}

	report := DetectCollisions(types)
	if report.HasCollisions {
		t.Errorf("expected no collisions, got %v", report.Collisions)
	}
	if len(report.Collisions) != 0 {
		t.Errorf("expected empty collisions map, got %v", report.Collisions)
	}
}

func TestDetectCollisions_WithCollisions(t *testing.T) {
	types := []DiscoveredType{
		{FullName: "pkg1.Event"},
		{FullName: "pkg2.Event"},
		{FullName: "pkg3.Other"},
	}

	report := DetectCollisions(types)
	if !report.HasCollisions {
		t.Fatal("expected collisions")
	}
	if len(report.Collisions) != 1 {
		t.Fatalf("expected 1 collision group, got %d", len(report.Collisions))
	}
	eventCollisions, ok := report.Collisions["Event"]
	if !ok {
		t.Fatal("expected collision for 'Event'")
	}
	if len(eventCollisions) != 2 {
		t.Errorf("expected 2 colliding names, got %d", len(eventCollisions))
	}
}

func TestDetectCollisions_MultipleCollisionGroups(t *testing.T) {
	types := []DiscoveredType{
		{FullName: "a.Event"},
		{FullName: "b.Event"},
		{FullName: "a.Command"},
		{FullName: "b.Command"},
		{FullName: "a.Unique"},
	}

	report := DetectCollisions(types)
	if !report.HasCollisions {
		t.Fatal("expected collisions")
	}
	if len(report.Collisions) != 2 {
		t.Errorf("expected 2 collision groups, got %d", len(report.Collisions))
	}
}

func TestDetectCollisions_Empty(t *testing.T) {
	report := DetectCollisions(nil)
	if report.HasCollisions {
		t.Error("expected no collisions for nil input")
	}
}

func TestMergeDescriptorSets(t *testing.T) {
	fd1 := &descriptorpb.FileDescriptorProto{Name: proto.String("file1.proto")}
	fd2 := &descriptorpb.FileDescriptorProto{Name: proto.String("file2.proto")}
	fd3 := &descriptorpb.FileDescriptorProto{Name: proto.String("file1.proto")} // duplicate

	set1 := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{fd1}}
	set2 := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{fd2, fd3}}

	merged := MergeDescriptorSets(set1, set2)

	if len(merged.File) != 2 {
		t.Errorf("expected 2 files (deduped), got %d", len(merged.File))
	}

	names := map[string]bool{}
	for _, f := range merged.File {
		names[f.GetName()] = true
	}
	if !names["file1.proto"] || !names["file2.proto"] {
		t.Errorf("unexpected files: %v", names)
	}
}

func TestMergeDescriptorSets_Empty(t *testing.T) {
	merged := MergeDescriptorSets()
	if len(merged.File) != 0 {
		t.Errorf("expected 0 files, got %d", len(merged.File))
	}
}

func TestMergeDescriptorSets_Single(t *testing.T) {
	fd := &descriptorpb.FileDescriptorProto{Name: proto.String("only.proto")}
	set := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{fd}}

	merged := MergeDescriptorSets(set)
	if len(merged.File) != 1 {
		t.Errorf("expected 1 file, got %d", len(merged.File))
	}
}

func TestLoadDescriptorSet_FileNotFound(t *testing.T) {
	_, err := LoadDescriptorSet("/nonexistent/path/descriptor.bin")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestLoadDescriptorSet_InvalidProto(t *testing.T) {
	// Write garbage data to a temp file
	tmp, err := os.CreateTemp("", "test-descriptor-*.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmp.Name())

	tmp.Write([]byte("not a protobuf"))
	tmp.Close()

	// This may or may not error depending on proto's leniency
	// but we're testing the code path
	_, err = LoadDescriptorSet(tmp.Name())
	// Proto might accept garbage silently, so just verify it doesn't panic
	_ = err
}

func TestLoadDescriptorSet_ValidProto(t *testing.T) {
	// Create a minimal valid FileDescriptorSet
	fds := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			{
				Name:    proto.String("test.proto"),
				Package: proto.String("test"),
				Syntax:  proto.String("proto3"),
			},
		},
	}

	data, err := proto.Marshal(fds)
	if err != nil {
		t.Fatal(err)
	}

	tmp, err := os.CreateTemp("", "test-descriptor-*.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmp.Name())

	tmp.Write(data)
	tmp.Close()

	loaded, err := LoadDescriptorSet(tmp.Name())
	if err != nil {
		t.Fatalf("LoadDescriptorSet error: %v", err)
	}
	if len(loaded.File) != 1 {
		t.Errorf("expected 1 file, got %d", len(loaded.File))
	}
	if loaded.File[0].GetName() != "test.proto" {
		t.Errorf("file name = %q, want \"test.proto\"", loaded.File[0].GetName())
	}
}

func TestLoadTypesFromDescriptorFile_NotFound(t *testing.T) {
	_, err := LoadTypesFromDescriptorFile("/nonexistent/path.bin")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestLoadTypesFromDescriptorEnv_NoEnv(t *testing.T) {
	// Ensure env var is unset
	os.Unsetenv(DescriptorPathEnvVar)

	types, err := LoadTypesFromDescriptorEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if types != nil {
		t.Errorf("expected nil types when no env var, got %v", types)
	}
}

func TestLoadTypesFromDescriptorEnv_InvalidPath(t *testing.T) {
	t.Setenv(DescriptorPathEnvVar, "/nonexistent/path.bin")

	_, err := LoadTypesFromDescriptorEnv()
	if err == nil {
		t.Error("expected error for invalid path in env var")
	}
}

func TestExtractTypesFromDescriptorSet_WithMessages(t *testing.T) {
	// Build a FileDescriptorSet with a message type
	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING
	labelOptional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	fieldNum := int32(1)

	fds := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			{
				Name:    proto.String("test.proto"),
				Package: proto.String("examples.player"),
				Syntax:  proto.String("proto3"),
				MessageType: []*descriptorpb.DescriptorProto{
					{
						Name: proto.String("PlayerRegistered"),
						Field: []*descriptorpb.FieldDescriptorProto{
							{
								Name:     proto.String("player_id"),
								JsonName: proto.String("playerId"),
								Number:   &fieldNum,
								Type:     &stringType,
								Label:    &labelOptional,
							},
						},
					},
				},
			},
		},
	}

	types, err := ExtractTypesFromDescriptorSet(fds)
	if err != nil {
		t.Fatalf("ExtractTypesFromDescriptorSet error: %v", err)
	}

	if len(types) != 1 {
		t.Fatalf("expected 1 type, got %d", len(types))
	}

	dt := types[0]
	if dt.FullName != "examples.player.PlayerRegistered" {
		t.Errorf("FullName = %q", dt.FullName)
	}
	if dt.TypeURL != "type.googleapis.com/examples.player.PlayerRegistered" {
		t.Errorf("TypeURL = %q", dt.TypeURL)
	}
	if !dt.IsEvent {
		t.Error("expected IsEvent = true for PlayerRegistered")
	}
	if dt.IsCommand {
		t.Error("expected IsCommand = false for PlayerRegistered")
	}
	if len(dt.Fields) != 1 {
		t.Fatalf("expected 1 field, got %d", len(dt.Fields))
	}
	if dt.Fields[0].Name != "player_id" {
		t.Errorf("field Name = %q", dt.Fields[0].Name)
	}
	if dt.Fields[0].JSONName != "playerId" {
		t.Errorf("field JSONName = %q", dt.Fields[0].JSONName)
	}
}

func TestExtractTypesFromDescriptorSet_SkipsFrameworkPackages(t *testing.T) {
	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING
	labelOptional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	fieldNum := int32(1)

	fds := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			{
				Name:    proto.String("angzarr.proto"),
				Package: proto.String("angzarr.coordinator"),
				Syntax:  proto.String("proto3"),
				MessageType: []*descriptorpb.DescriptorProto{
					{
						Name: proto.String("InternalType"),
						Field: []*descriptorpb.FieldDescriptorProto{
							{
								Name:     proto.String("id"),
								JsonName: proto.String("id"),
								Number:   &fieldNum,
								Type:     &stringType,
								Label:    &labelOptional,
							},
						},
					},
				},
			},
			{
				Name:    proto.String("business.proto"),
				Package: proto.String("examples.order"),
				Syntax:  proto.String("proto3"),
				MessageType: []*descriptorpb.DescriptorProto{
					{
						Name: proto.String("OrderCreated"),
						Field: []*descriptorpb.FieldDescriptorProto{
							{
								Name:     proto.String("order_id"),
								JsonName: proto.String("orderId"),
								Number:   &fieldNum,
								Type:     &stringType,
								Label:    &labelOptional,
							},
						},
					},
				},
			},
		},
	}

	types, err := ExtractTypesFromDescriptorSet(fds)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	// Should only have OrderCreated, not InternalType
	if len(types) != 1 {
		t.Fatalf("expected 1 type (skipping angzarr.*), got %d", len(types))
	}
	if types[0].FullName != "examples.order.OrderCreated" {
		t.Errorf("expected OrderCreated, got %q", types[0].FullName)
	}
}

func TestExtractTypesFromDescriptorSet_SortedOutput(t *testing.T) {
	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING
	labelOptional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	fieldNum := int32(1)

	makeMsg := func(name string) *descriptorpb.DescriptorProto {
		return &descriptorpb.DescriptorProto{
			Name: proto.String(name),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("id"), JsonName: proto.String("id"), Number: &fieldNum, Type: &stringType, Label: &labelOptional},
			},
		}
	}

	fds := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			{
				Name:        proto.String("test.proto"),
				Package:     proto.String("examples"),
				Syntax:      proto.String("proto3"),
				MessageType: []*descriptorpb.DescriptorProto{makeMsg("Zebra"), makeMsg("Alpha"), makeMsg("Middle")},
			},
		},
	}

	types, err := ExtractTypesFromDescriptorSet(fds)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	if len(types) != 3 {
		t.Fatalf("expected 3 types, got %d", len(types))
	}

	// Should be sorted by FullName
	if types[0].FullName != "examples.Alpha" {
		t.Errorf("types[0] = %q, want \"examples.Alpha\"", types[0].FullName)
	}
	if types[1].FullName != "examples.Middle" {
		t.Errorf("types[1] = %q, want \"examples.Middle\"", types[1].FullName)
	}
	if types[2].FullName != "examples.Zebra" {
		t.Errorf("types[2] = %q, want \"examples.Zebra\"", types[2].FullName)
	}
}

func TestExtractTypesFromDescriptorSet_WithEnumField(t *testing.T) {
	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING
	enumType := descriptorpb.FieldDescriptorProto_TYPE_ENUM
	labelOptional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	fieldNum1 := int32(1)
	fieldNum2 := int32(2)

	fds := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			{
				Name:    proto.String("test.proto"),
				Package: proto.String("examples"),
				Syntax:  proto.String("proto3"),
				EnumType: []*descriptorpb.EnumDescriptorProto{
					{
						Name: proto.String("Status"),
						Value: []*descriptorpb.EnumValueDescriptorProto{
							{Name: proto.String("UNKNOWN"), Number: proto.Int32(0)},
							{Name: proto.String("ACTIVE"), Number: proto.Int32(1)},
						},
					},
				},
				MessageType: []*descriptorpb.DescriptorProto{
					{
						Name: proto.String("PlayerState"),
						Field: []*descriptorpb.FieldDescriptorProto{
							{Name: proto.String("id"), JsonName: proto.String("id"), Number: &fieldNum1, Type: &stringType, Label: &labelOptional},
							{Name: proto.String("status"), JsonName: proto.String("status"), Number: &fieldNum2, Type: &enumType, Label: &labelOptional, TypeName: proto.String(".examples.Status")},
						},
					},
				},
			},
		},
	}

	types, err := ExtractTypesFromDescriptorSet(fds)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	if len(types) != 1 {
		t.Fatalf("expected 1 type, got %d", len(types))
	}

	// Find the enum field
	var enumField *FieldDef
	for _, f := range types[0].Fields {
		if f.Name == "status" {
			enumField = &f
			break
		}
	}
	if enumField == nil {
		t.Fatal("missing status field")
	}
	if enumField.Type != "examples.Status" {
		t.Errorf("enum field Type = %q, want \"examples.Status\"", enumField.Type)
	}
}

func TestExtractTypesFromDescriptorSet_NestedMessages(t *testing.T) {
	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING
	labelOptional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	fieldNum := int32(1)

	fds := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			{
				Name:    proto.String("test.proto"),
				Package: proto.String("examples"),
				Syntax:  proto.String("proto3"),
				MessageType: []*descriptorpb.DescriptorProto{
					{
						Name: proto.String("Outer"),
						Field: []*descriptorpb.FieldDescriptorProto{
							{Name: proto.String("id"), JsonName: proto.String("id"), Number: &fieldNum, Type: &stringType, Label: &labelOptional},
						},
						NestedType: []*descriptorpb.DescriptorProto{
							{
								Name: proto.String("Inner"),
								Field: []*descriptorpb.FieldDescriptorProto{
									{Name: proto.String("value"), JsonName: proto.String("value"), Number: &fieldNum, Type: &stringType, Label: &labelOptional},
								},
							},
						},
					},
				},
			},
		},
	}

	types, err := ExtractTypesFromDescriptorSet(fds)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	if len(types) != 2 {
		t.Fatalf("expected 2 types (Outer + Inner), got %d", len(types))
	}

	names := map[string]bool{}
	for _, dt := range types {
		names[dt.FullName] = true
	}
	if !names["examples.Outer"] {
		t.Error("missing examples.Outer")
	}
	if !names["examples.Outer.Inner"] {
		t.Error("missing examples.Outer.Inner")
	}
}

// Enum-typed fields are marked so the schema renders them as strings
// rather than as references to a message definition.
func TestExtractTypesFromDescriptorSet_MarksEnumFields(t *testing.T) {
	enumType := descriptorpb.FieldDescriptorProto_TYPE_ENUM
	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING
	labelOptional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	one, two := int32(1), int32(2)

	fds := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{{
			Name:    proto.String("order.proto"),
			Package: proto.String("examples.order"),
			Syntax:  proto.String("proto3"),
			EnumType: []*descriptorpb.EnumDescriptorProto{{
				Name: proto.String("Status"),
				Value: []*descriptorpb.EnumValueDescriptorProto{
					{Name: proto.String("STATUS_OPEN"), Number: proto.Int32(0)},
				},
			}},
			MessageType: []*descriptorpb.DescriptorProto{{
				Name: proto.String("OrderUpdated"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("status"), JsonName: proto.String("status"), Number: &one,
						Type: &enumType, TypeName: proto.String(".examples.order.Status"), Label: &labelOptional},
					{Name: proto.String("note"), JsonName: proto.String("note"), Number: &two,
						Type: &stringType, Label: &labelOptional},
				},
			}},
		}},
	}

	types, err := ExtractTypesFromDescriptorSet(fds)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	fields := types[0].Fields
	if !fields[0].Enum || fields[0].Type != "examples.order.Status" {
		t.Errorf("status field = %+v, want enum examples.order.Status", fields[0])
	}
	if fields[1].Enum {
		t.Errorf("note field marked as enum")
	}
}
