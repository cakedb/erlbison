# erlbison

Erlbison is an Erlang library to work with [BSON](http://bsonspec.org/) files.
Its discerning feature is attempt to do as much work as possible at the
binary level, without translating the data to native Erlang data structure
unless when absolutely necessary, in order to maximize performance.

## Usage

Erlbison exposes six functions:

`Foo = bson:load("filename.bson").`

loads the BSON, validates that it has proper BSON format, and binds Foo to the
resulting binary.

`Bar = bson:validate(Foo).`

returns the atom `true` or `false` depending if binary Foo has proper BSON format.

`Baz = bson:parse(Foo).`

returns a proplist with the parsed content of BSON binary Foo.

`Bing = bson:filter(Foo, [Key1, ..., KeyN]).`

returns a subset of FOO (a proper BSON binary) that contains strictly the elements identified by
the keys contained in the list in the second argument of `bson:filter/2`.

`Bong = bson:safe_search(Foo, [{Key1, Value1}, ..., {KeyN, ValueN}]).`

`bson:safe_search/2` parses the BSON binary Foo into a proplist, and returns either Foo or 
and empty BSON depending whether this parsed Foo contains every key-value pair stipulated in the second
argument.

`Bam = bson:fast_search(Foo, [{Key1, Value1}, ..., {KeyN, ValueN}]).`

`bson:fast_search/2` encodes every key-value pair in its second argument into binaries, and returns either Foo or 
and empty BSON depending whether Foo contains every encoded key-value pair.

## Limitation of `bson:fast_search/2`

The BSON format explicitely store data types and array indices, which makes it difficult (impossible in
certain cases) to do searches without explicitely stating desired types. Because of this, `bson:fast_search/2`
does not support the following types as of now:

* Datetime (confused for 64-bits integers)
* Timestamp (confused for 32-bits integers)
* Regex (confused for strings)
* Javascript code (confused for strings)
* Arrays (BSON uses arbitrary explicit indices)

Furthermore, certain types might be confused with others:

* 64-Integers whose absolute value is smaller than 2^31
* Lists of integers deemed printable by Erlang (e.g. [100,100,100])
* 12-bytes binaries will be confused for BSON ObjectIDs

If you need to do a search with the above types, use `bson:safe_search/2` instead.

## Test suite

Comprehensive Eunit tests can be run by running these commands from the root folder of Erlbison's repository\*:

`erlbison $ erl -pa src/ -pa test/`

`> eunit:test(bson).`

\* If for some reason the test/test.bson file is lost, it can be reconstructed by running

`erlbison $ python test/generate_bson.py`

## TODO

* "In"-documents searches
* Investigate the arbitrary array indexing

