io = { $path }: { $ext_err_txt }
relative-path = { $path } is a relative path; a lock is only ever taken at an absolute one
not-regular = the lock { $path } is not a regular file, so it cannot be locked
timeout = the lock { $path } is still held after { $waited_ms }ms: another ctxloom, taskloom or ltk process holds it; try again once it exits
replaced = the lock { $path } was replaced { $attempts } times while being taken: try again
