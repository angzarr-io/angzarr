# Messages for angzarr-proto-jsonl's errors. Each id is an error's code
# without the crate's name, which the build prefixes (angzarr_diagnostic::build);
# the variables are its args. Messages are lowercase fragments, like the
# English Display, so they read well inside another message. $line is
# 1-based; 0 when the text did not come from a stream. $ext_err_txt is a
# third party's text (serde_json's, the reader's I/O), verbatim.

encode-failed = the message can't be written as JSON: { $ext_err_txt }
malformed-line = line { $line }: not a message of the type being read: { $ext_err_txt }
blank-line = line { $line }: a blank line (every line holds one message)
not-one-line = text holds a line break, so it is more than one line
not-utf8 = line { $line }: not UTF-8 text
torn-tail = line { $line }: the last line has no line feed ({ $bytes } bytes), so the write that made it may have been cut short
read-failed = line { $line }: the stream can't be read: { $ext_err_txt }
write-failed = the stream can't be written: { $ext_err_txt }
