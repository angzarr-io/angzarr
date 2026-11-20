# Messages for angzarr-coordinator-base's errors. Each id is an UpcastError
# code without the crate's name, which the build prefixes
# (angzarr_diagnostic::build); the variables are its args. $cause is the
# upcaster's own failure, rendered by its own message; $reason is the key an
# upcaster refused under, rendered from the catalog of the crate that owns
# the key (angzarr_proto::REASONS_EN_US, for a core key).
# The crate exports this as angzarr_coordinator_base::EN_US.

upcast-unavailable = the upcaster cannot be reached: { $cause }
upcast-refused = the upcaster refused: { $reason }
