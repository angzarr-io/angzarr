# Messages for angzarr-sealed's errors. Each id is an error's code without
# the crate's name, which the build prefixes (coded_diagnostic::build);
# the variables are its args. Messages are lowercase fragments.

## Opening a Sealed.

open-failed = the sealed value could not be opened with this key (it was altered, made for another key or other data, or made with another key)
open-unsupported-algorithm = the sealed value names an algorithm this crate does not implement
open-malformed = the sealed value is malformed: { $field } is { $why }
open-key-destroyed = the data key was destroyed
open-unknown-key = the key set holds no key named "{ $key_id }"

## A key set.

key-set-no-such-key = the key set holds no key named "{ $key_id }"
key-set-already-held = the key set already holds a key named "{ $key_id }": a key is not replaced, add the new one under a new id
key-set-current-key = "{ $key_id }" is the current key: mark another current before removing it

## Sealing.

seal-weak-shared-secret = the recipient key is a low-order point: the shared secret is all zero
seal-rng-failed = the system random number generator failed
seal-key-destroyed = the data key was destroyed
seal-encryption-failed = the AEAD refused to encrypt: the plaintext is too long, or the sequence numbers are exhausted
seal-too-long = { $field } is longer than 4294967295 bytes, which the associated data cannot length-prefix

## Wrapping a data key.

wrap-failed = the wrapped data key could not be opened with this key-encryption key (it was altered, or made for another key or subject)
wrap-malformed = the wrapped key is malformed: { $field } is { $why }
