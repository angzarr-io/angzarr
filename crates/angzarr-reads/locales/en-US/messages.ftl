# Messages for angzarr-reads's errors. Each id is an Invalid code without
# the crate's name, which the build prefixes (angzarr_diagnostic::build);
# the variables are its args. The crate exports this as
# angzarr_reads::EN_US.

no-domain = the stream's domain is not named
bad-root = the stream's root is not 16 bytes
empty-edition = the edition is named, and its name is empty
bad-correlation = the correlation id is unset or not 16 bytes
no-target = a read of a domain names neither a stream's root nor a correlation
many-streams = one book was asked for and { $streams } streams answer
too-large = the read needs { $books } books and one is allowed: { $pages } pages of at most { $max_pages } pages and { $max_bytes } bytes a book
