# Messages for the DuckDB adapter's errors, carried in an angzarr-ports
# StoreError as a cause's code and args. A third party's text (DuckDB's,
# prost's) is the arg ext_err_txt, verbatim. Each id is a code without the
# crate's name, which the build prefixes (angzarr_diagnostic::build).

unavailable = the database failed: { $ext_err_txt }
held-elsewhere = the database file is held by another process or handle: { $ext_err_txt }
undecodable = a stored value does not decode: { $ext_err_txt }
bad-root = a stored root of { $length } bytes, not a UUID's 16
bad-dead-letter-place = a dead letter's stored place of kind { $kind } and { $length } bytes is neither a sequence nor a command id
newer-format = the database file is of format { $found }, newer than the { $supported } this build reads
not-a-store = the database file holds tables but not this store's: { $ext_err_txt }
damaged =
    the database file is inconsistent: { $first }{ $more ->
        [0] {""}
        [one] {" "}(and 1 more problem)
       *[other] {" "}(and { $more } more problems)
    }
