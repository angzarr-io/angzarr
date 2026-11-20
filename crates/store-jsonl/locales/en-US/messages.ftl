# Messages for store-jsonl's diagnostics. Each id is a code without the
# crate's name, which the build prefixes (angzarr_diagnostic::build); the
# variables are its args.

relative-path = the journal path { $path } is relative; give the absolute path of the journal
in-use = the journal { $path } is held by another store: a journal has one store at a time, in one process; close that store first, or use a store that several processes can share
lock-failed = the lock beside the journal { $path } could not be taken: { $ext_err_txt }
io = the journal { $path } could not be read or written: { $ext_err_txt }
corrupt = line { $line } of the journal { $path } is not an entry of this format ({ $reason }); the journal is left as it is: repair or restore it
not-a-journal = { $path } does not begin with a journal header, so it is not a journal of this store
unsupported-format = the journal { $path } is in format { $found }; this build reads formats up to { $supported }
replay-refused = line { $line } of the journal { $path } records an operation the store refuses ({ $reason }); the journal was not written by this store, or was edited
torn-tail-truncated = the last line of the journal { $path } (line { $line }, { $bytes } bytes) was cut short, so it never completed; it was removed and nothing before it was touched
encode-failed = the operation could not be written as a journal line, so it was not attempted: { $ext_err_txt }
append-failed = the operation could not be appended to the journal, so whether it is there is unknown: { $ext_err_txt }; the store answers nothing more: open the journal again to see what it holds
poisoned = an earlier append to the journal failed, so the store answers nothing more: open the journal again
suspended = the in-memory store did not complete at once, which it always does: this is a bug in store-jsonl
