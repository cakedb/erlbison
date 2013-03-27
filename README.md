# erlbison

Erlbison is an Erlang library to work with [BSON](http://bsonspec.org/) files.
Its discerning feature is its attempt to do as much work as possible at the
binary level, without translating the data to native Erlang data structure
unless when absolutely necessary, in order to maximize performance.

## Usage

Erlbison exposes five functions:

`Foo = bson:load("filename.bson").`

loads the BSON, validates that it has proper BSON format, and binds Foo to the
resulting binary.

`Bar = bson:validate(Foo).`

returns the atom `true` or `false` depending if binary Foo has proper BSON format.

`Baz = bson:parse(Foo).`

returns a native Erlang proplist with the parsed content of BSON binary Foo.

`Bing = bson:filter(Foo, [Key1, ..., KeyN]).`

returns a subset of FOO (a proper BSON binary) that contains strictly the elements identified by
the keys contained in the list in the second argument of `bson:filter/2`.

`Foo = bson:search(Foo, [{Key1, Value1}, ..., {KeyN, ValueN}]).`

returns the BSON Foo if and only if it contains all the key-value pairs listed in its second argument.

## Test suite

Comprehensive Eunit tests can be run by running these commands from the root folder of Erlbison's repository\*:

`erlbison $ erl -pa src/ -pa test/`

`> eunit:test(bson).`

\* If for some reason the test/test.bson file is lost, it can be reconstructed by running

`erlbison $ python test/generate_bson.py`

## TODO

* More thorough testing, especially with JSCODEWS type
* "In"-documents searches

