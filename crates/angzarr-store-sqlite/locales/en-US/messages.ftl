# Messages for the SQLite adapter's errors, carried in an angzarr-ports
# StoreError as a cause's code and args. A third party's text (SQLite's, the
# driver's, prost's) is the arg ext_err_txt, verbatim; SQLite's primary
# result code is errno, where SQLite answered: a driver failure without one
# is driver-failed. A Damage, which an integrity check returns, has its
# args. Each id is a code without the crate's name, which the build prefixes
# (angzarr_diagnostic::build).

unavailable = the database failed (SQLite code { $errno }): { $ext_err_txt }
driver-failed = the database driver failed: { $ext_err_txt }
undecodable = a stored value does not decode: { $ext_err_txt }
backup-uncreatable = the backup { $path } could not be created: { $ext_err_txt }
damaged-file = the database file is too damaged to open (SQLite code { $errno }): { $ext_err_txt }
no-data-version = the database gave no data version
bad-root = a stored root of { $length } bytes, not a UUID's 16
bad-dead-letter-place = a dead letter's stored place of kind { $kind } and { $length } bytes is neither a sequence nor a command id
damaged =
    the database file is damaged: { $first }{ $more ->
        [0] {""}
        [one] {" "}(and 1 more problem)
       *[other] {" "}(and { $more } more problems)
    }
