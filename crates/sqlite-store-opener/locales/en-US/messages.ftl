# Messages for the crate's errors. Each id is an error's code without the
# crate's name, which the build prefixes (angzarr_diagnostic::build); the
# variables are its args. An error that carries another crate's (the project
# id's, the store's) is rendered by that crate's catalog.

create-dir = cannot create the store's directory { $path }: { $ext_err_txt }
unplaced = cannot find where the store { $path } is on disk: { $ext_err_txt }
predates-embedded = the store { $path } predates angzarr-embedded: move it to { $backup } to start afresh
restrict = cannot make the store's { $path } private to its owner, so it is not opened: { $ext_err_txt }
