// Package discovery provides proto type discovery from FileDescriptorSet files.
package discovery

// DiscoveredType represents a proto message type.
type DiscoveredType struct {
	// FullName is the fully qualified proto type name (e.g., "examples.player.PlayerRegistered")
	FullName string
	// TypeURL is the Any type URL (e.g., "type.googleapis.com/examples.player.PlayerRegistered")
	TypeURL string
	// Fields contains the message field definitions
	Fields []FieldDef
	// IsEvent indicates if this looks like an event type (past tense naming)
	IsEvent bool
	// IsCommand indicates if this looks like a command type (imperative naming)
	IsCommand bool
}

// FieldDef describes a proto message field.
type FieldDef struct {
	Name     string
	JSONName string
	Type     string
	Repeated bool
	Optional bool
	// Enum is true when Type names a proto enum rather than a message.
	Enum bool
}

// isEventName checks if name looks like an event (past tense)
func isEventName(name string) bool {
	suffixes := []string{"ed", "Created", "Updated", "Deleted", "Changed", "Completed", "Failed", "Started", "Ended"}
	for _, s := range suffixes {
		if len(name) > len(s) && name[len(name)-len(s):] == s {
			return true
		}
	}
	return false
}

// isCommandName checks if name looks like a command (imperative)
func isCommandName(name string) bool {
	prefixes := []string{"Create", "Update", "Delete", "Start", "Stop", "Register", "Execute", "Handle", "Process", "Deposit", "Withdraw", "Reserve", "Release"}
	for _, p := range prefixes {
		if len(name) >= len(p) && name[:len(p)] == p {
			return true
		}
	}
	return false
}
