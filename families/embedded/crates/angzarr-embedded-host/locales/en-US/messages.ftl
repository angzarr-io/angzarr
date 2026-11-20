# Messages for angzarr-embedded-host's errors. Each id is an error's code
# without the crate's name, which the build prefixes
# (angzarr_diagnostic::build); the variables are its args. Messages are
# lowercase fragments, like the English Display, so they read well inside
# another message. A condition the spec keys is coded under its key, whose
# English is the spec's catalog (angzarr_proto::REASONS_EN_US; decision
# 14.60), never here.

duplicate-domain = component "{ $domain }" is configured twice
no-launch = component "{ $domain }": language "{ $language }" has no default launch command; give one
unknown-language = the config's "{ $key }" names language "{ $language }", which is not loaded; the languages loaded are { $loaded }
empty-launch = component "{ $domain }": the launch command is empty
spawn = component "{ $domain }": could not start "{ $program }" ({ $kind })
describe = component "{ $domain }": Describe failed with gRPC code { $grpc_code }
kind-mismatch = component "{ $domain }" is configured as a { $configured } but describes itself as { $described }
upcaster-no-domain = component "{ $domain }" is an upcaster that names no domain to upcast: give it `upcasts`
not-exposable = component "{ $domain }" is a { $kind }: only an aggregate may be exposed
not-exposed = a request for "{ $domain }" from { $arrival } was answered as an unknown domain: its component is exposed { $expose }
descriptor = component "{ $domain }": its descriptor is refused: { $cause }
descriptor-changed = component "{ $domain }" restarted describing another component than it is registered as: restart the host to take the change
runtime = the host's runtime could not be built ({ $kind })
socket-dir-too-long = socket_dir "{ $dir }" is too long for a Unix socket path: at most { $max_bytes ->
        [one] { $max_bytes } byte
       *[other] { $max_bytes } bytes
    }
config-unreadable = the config { $path } could not be read ({ $kind })
config-invalid = the config { $path } is not valid: { $cause }
not-registered = component "{ $domain }" is listed as an in-process { $kind }, but the application registered none
not-listed = the application registered the in-process { $kind } "{ $domain }", but no component entry lists it
register = a component could not be registered: { $cause }
store = the store could not be opened: { $cause }