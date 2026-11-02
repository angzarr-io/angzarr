# Messages for angzarr-presentation's errors. Each id is an error's code
# without the crate's name, which the build prefixes
# (angzarr_diagnostic::build); the variables are its args.

unknown-format = the format { $value } is not one of text or json
catalog-invalid = the { $source } message catalog cannot be loaded: { $ext_err_txt }
unknown-code = no catalog has a message for { $code }
unformattable = the message for { $code } cannot be filled in: { $ext_err_txt }
