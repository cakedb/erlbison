-module(bson).

-export([load/1, validate/1, parse/1, filter/2, search/2]).

% BSON types
-define(BSON_EOD, 0).
-define(BSON_DOUBLE, 1).
-define(BSON_STRING, 2).
-define(BSON_DOCUMENT, 3).
-define(BSON_ARRAY, 4).
-define(BSON_BINARY, 5).
-define(BSON_OBJECTID, 7).
-define(BSON_BOOL, 8).
-define(BSON_DATETIME, 9).
-define(BSON_NULL, 10).
-define(BSON_REGEX, 11).
-define(BSON_JSCODE, 13).
-define(BSON_JSCODEWS, 15).
-define(BSON_INT32, 16).
-define(BSON_TS, 17).
-define(BSON_INT64, 18).
-define(BSON_MAXKEY, 127).
-define(BSON_MINKEY, 255).

-define(BSON, <<_Length:32/little-integer, Payload/binary>>).
-define(EMPTY_BSON, <<5,0,0,0,0>>).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% CLIENT API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

validate(?BSON) ->
    case size(?BSON) of
        _Length ->
            validate(payload, Payload);
        _ ->
            false
    end;
validate(_) ->
    false.

load(Filename) ->
    {ok, Document} = file:read_file(Filename),
    case validate(Document) of
        true ->
            Document;
        false ->
            {error, ill_formed_bson}
    end.

parse(?BSON) ->
    parse(Payload, []).

filter(?BSON, Keys)->
    KeysBinary = [list_to_binary(K) || K <- Keys],
    Result = filter(Payload, KeysBinary, <<>>),
    Size = size(Result),
    <<(Size+5):32/little-integer, Result:Size/binary, 0>>.

search(?BSON, Queries) ->
    QueriesParsed = [{list_to_binary(K),V} || {K,V} <- Queries],
    case satisfies(Payload, QueriesParsed) of
        true ->
            ?BSON;
        false ->
            ?EMPTY_BSON
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% PRIVATE FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% get_key chops of the key out of a BSON binary
% and returns it with the remainder of the binary
get_key(Data) ->
    get_key(Data,<<>>).

get_key(<<0, Remainder/binary>>, Key) ->
    {Key, Remainder};
get_key(<<Char, Remainder/binary>>, Key) ->
    get_key(Remainder, <<Key/binary, Char>>);
get_key(<<>>, _Key) ->
    error.

% chop bites off a number of bits from
% the front of a binary. It deals with three cases
% (i) when the number of bits to chop is
% encoded at the front of the binary, including
% itself, (ii) when the number of bits to
% chop is encoded at the front of the binary,
% excluding itself and (iii) when the number
% of bites is predefined by the caller.
chop(inclusive, Bits, Payload) ->
    <<Size:Bits/little-integer, Rest/binary>> = Payload,
    Length = Size - 4,
    <<Value:Length/binary, Remainder/binary>> = Rest,
    {<<Size:Bits/little-integer>>, Value, Remainder};
chop(exclusive, Bits, Payload) ->
    <<Length:Bits/little-integer, Value:Length/binary, Remainder/binary>> = Payload,
    {<<Length:Bits/little-integer>>, Value, Remainder}.

chop(Size, Payload) ->
    <<Value:Size/binary, Remainder/binary>> = Payload,
    {<<>>, Value, Remainder}.

% get_value/2 takes a integer representing
% a BSON datatype, as well as a binary,
% and return a 3-tuple of binaries holding
% (i) the size of the value, (ii) the value
% itself and (iii) the "tail" of the binary.
get_value(?BSON_DOUBLE, Payload) ->
    chop(8, Payload);
get_value(?BSON_STRING, Payload) ->
    chop(exclusive, 32, Payload);
get_value(?BSON_DOCUMENT, Payload) ->
    {Prefix, Value, Remainder} = chop(inclusive, 32, Payload),
    {<<>>, <<Prefix/binary, Value/binary>>, Remainder};
get_value(?BSON_ARRAY, Payload) ->
    {Prefix, Value, Remainder} = chop(inclusive, 32, Payload),
    {<<>>, <<Prefix/binary, Value/binary>>, Remainder};
get_value(?BSON_BINARY, Payload) ->
    chop(exclusive, 40, Payload);
get_value(?BSON_OBJECTID, Payload) ->
    chop(12, Payload);
get_value(?BSON_BOOL, Payload) ->
    chop(1, Payload);
get_value(?BSON_DATETIME, Payload) ->
    chop(8, Payload);
get_value(?BSON_NULL, Payload) ->
    chop(0, Payload);
get_value(?BSON_REGEX, Payload) ->
    {Regex, Tail} = get_key(Payload),
    case Tail of
        <<>> ->
            {<<>>, Regex, <<0>>};
        <<0, Remainder/binary>> ->
            {<<>>, Regex, Remainder}
    end;
get_value(?BSON_JSCODE, Payload) ->
    chop(exclusive, 32, Payload);
get_value(?BSON_JSCODEWS, Payload) ->
    chop(inclusive, 32, Payload);
get_value(?BSON_INT32, Payload) ->
    chop(4, Payload);
get_value(?BSON_TS, Payload) ->
    chop(8, Payload);
get_value(?BSON_INT64, Payload) ->
    chop(8, Payload);
get_value(?BSON_MINKEY, Payload) ->
    chop(0, Payload);
get_value(?BSON_MAXKEY, Payload) ->
    chop(0, Payload);
get_value(_, _Payload) ->
    error.

% decode/2 takes an integer representing
% a BSON datatype as well a a binary
% and translate this binary into a native
% Erlang type.
decode(?BSON_DOUBLE, <<Double:64/little-signed-float>>) ->
    Double;
decode(?BSON_STRING, <<Length:32/little-integer, Payload:Length/binary>>) ->
    Size = Length - 1,
    <<String:Size/binary, 0>> = Payload,
    binary_to_list(String);
decode(?BSON_DOCUMENT, Value) ->
    parse(Value);
decode(?BSON_ARRAY, Binary) ->
    [Value || {_Key, Value} <- lists:reverse(parse(Binary))];
decode(?BSON_BINARY, <<Length:40/little-integer, Binary:Length/binary>>) ->
    Binary;
decode(?BSON_OBJECTID, ObjectId) ->
    ObjectId;
decode(?BSON_BOOL, <<1>>) ->
    true;
decode(?BSON_BOOL, <<0>>) ->
    false;
decode(?BSON_DATETIME, <<Datetime:64/little-integer>>) ->
    Datetime;
decode(?BSON_NULL, <<>>) ->
    null;
decode(?BSON_REGEX, Regex) ->
    binary_to_list(Regex);
decode(?BSON_JSCODE, <<Length:32/little-integer, Payload:Length/binary>>) ->
    Size = Length - 1,
    <<Code:Size/binary, _/binary>> = Payload,
    binary_to_list(Code);
decode(?BSON_JSCODEWS, <<_:32/little-integer, Payload/binary>>) ->

    % Parse code 
    <<CodeLength:32/little-integer, A:CodeLength/binary, Scope/binary>> = Payload,
    CodeSize = CodeLength - 1,
    <<Code:CodeSize/binary, _/binary>> = A,

    % Parse scope 
    {_, <<_:1/binary, C/binary>>, _} = chop(inclusive, 32, Scope),
    {ScopeID, Value} = get_key(C),
    {<<Length:32/little-integer>>, D, _} = chop(exclusive, 32, Value),
    Size = Length -1,
    <<ScopeValue:Size/binary, _/binary>> = D,

    {binary_to_list(Code), binary_to_list(ScopeID), binary_to_list(ScopeValue)};
decode(?BSON_INT32, <<Value:32/little-signed-integer>>) ->
    Value;
decode(?BSON_TS, <<Increment:32/little-integer, Seconds:32/little-integer>>) ->
    {Increment, Seconds};
decode(?BSON_INT64, <<Value:64/little-signed-integer>>) ->
    Value;
decode(?BSON_MINKEY, _Value) ->
    minkey;
decode(?BSON_MAXKEY, _Value) ->
    maxkey;
decode(_, _Value) ->
    error.

% encode/2 takes an integer representing
% a BSON datatype as well a value 
% represented inside a native Erlang
% datatype and encode it to a binary
% according to the BSON specs
encode(?BSON_DOUBLE, Value) ->
    <<Value:64/little-signed-float>>;
encode(?BSON_STRING, Value) ->
    BitString = list_to_binary(Value),
    <<BitString/binary,0>>;
encode(?BSON_BINARY, Value) ->
    Value;
encode(?BSON_OBJECTID, Value) ->
    Value;
encode(?BSON_BOOL, false) ->
    <<0>>;
encode(?BSON_BOOL, true) ->
    <<1>>;
encode(?BSON_DATETIME, Value) ->
    <<Value:64/little-signed-integer>>;
encode(?BSON_NULL, _) ->
    <<>>;
encode(?BSON_REGEX, Value) ->
    list_to_binary(Value);
encode(?BSON_JSCODE, Value) ->
    Code = list_to_binary(Value),
    <<Code/binary,0>>;
encode(?BSON_JSCODEWS, {Code, ScopeID, ScopeValue}) ->
    CodeBinary = list_to_binary(Code),
    LengthCode = size(CodeBinary) + 1,
    ScopeIDBinary = list_to_binary(ScopeID),
    ScopeValueBinary = list_to_binary(ScopeValue),
    LengthValue = size(ScopeValueBinary) + 1,
    Temp = <<2, ScopeIDBinary/binary, 0, LengthValue:32/little-integer, ScopeValueBinary/binary, 0, 0>>,
    Length = size(Temp) + 4,
    <<LengthCode:32/little-integer, CodeBinary/binary, 0, Length:32/little-integer, Temp/binary>>;
encode(?BSON_INT32, Value) ->
    <<Value:32/little-signed-integer>>;
encode(?BSON_TS, {Increment, Seconds}) ->
    <<Increment:32/little-integer, Seconds:32/little-integer>>;
encode(?BSON_INT64, Value) ->
    <<Value:64/little-signed-integer>>;
encode(?BSON_MINKEY, _) ->
    <<>>;
encode(?BSON_MAXKEY, _) ->
    <<>>.

% pop takes a single element off the payload
% binary and returns a 5-tuple of the binary
% with (i) the element's BSON datatype,
% (ii) key, (iii) prefix (often the length
% of the value binary), (iv) the value and
% (v) the binary's "tail"
pop(<<0>>) ->
    eod;
pop(<<Datatype, Payload/binary>>) ->
    {Key, Rest} = get_key(Payload),
    {Prefix, Value, Remainder} = get_value(Datatype, Rest),
    {Datatype, Key, Prefix, Value, Remainder}.

% validate/2 returns a boolean whether a binary
% is formed according to the BSON specs or not
validate(payload, Payload) ->
    case pop(Payload) of
        eod->
            true;
        {_Datatype, error, _Prefix, _Value, _Remainder} ->
            false;
        {_Datatype, _Key, _Prefix, error, _Remainder} ->
            false;
        {_Datatype, _Key, _Prefix, _Value, Remainder} ->
            validate(payload, Remainder)
    end.

% parse/2 returns a native Erlang proplist containing
% the keyvalue elements inside a BSON binary
parse(<<0>>, List) ->
    List;
parse(Payload, List) ->
    {Datatype, Key, Prefix, Value, Remainder} = pop(Payload),
    parse(Remainder, [{binary_to_list(Key), decode(Datatype, <<Prefix/binary, Value/binary>>)} | List]).

% filter/3 takes a binary holding BSON arguments as
% well as a list of keys and returns the subset of
% elements having the predefined keys
filter(<<0>>, _Keys, Result) ->
    Result;
filter(_Payload, [], Result) ->
    Result;
filter(Payload, Keys, Result) ->
    {Datatype, Key, Prefix, Value, Remainder} = pop(Payload),
    case lists:member(Key, Keys) of
        true ->
            Element = <<Datatype, Key/binary, 0, Prefix/binary, Value/binary>>,
            filter(Remainder, lists:delete(Key, Keys), <<Result/binary, Element/binary>>); 
        false ->
            filter(Remainder, Keys, Result) 
    end.

% satisfies/2 takes a binary containing BSON elements
% as well as a proplist of queries. The queries are
% either {key, value} or {key, lambda} where lambda
% is a function with arity of one which returns a boolean
satisfies(_Payload, []) ->
    true;
satisfies(<<0>>, _Queries) ->
    false;
satisfies(Payload, Queries) ->
    {Datatype, Key, _Prefix, Value, Remainder} = pop(Payload),
    case lists:keyfind(Key, 1, Queries) of
        {Key, Comparator} when is_function(Comparator) ->
            % MORE WORKS NEED TO BE DONE HERE
            satisfies(Remainder, lists:delete({Key, Comparator}, Queries));
        {Key, Val} ->
            case equal(Datatype, Val, Value) of
                true ->
                    satisfies(Remainder, lists:delete({Key, Val}, Queries));
                false ->
                    satisfies(Remainder, Queries)
            end;
        false ->
            satisfies(Remainder, Queries)
    end.

% equal/3 takes a datatype, a query
% (in native Erlang type) and a value
% (in binary BSON) and returns a boolean
% whether the query and the value are
% the same
equal(?BSON_DOCUMENT, KeyValues, ?BSON) ->
    KeyValuesParsed = [{list_to_binary(K), V} || {K,V} <- KeyValues],
    % MORE WORK HERE FOR DOCUMENTS OF DIFF. LENGTH
    satisfies(Payload, KeyValuesParsed);
equal(?BSON_ARRAY, Array, ?BSON) ->
    compare(Array, Payload);
equal(Datatype, Query, Value) ->
    case encode(Datatype, Query) of
        Value ->
            true;
        _ ->
            false
    end.

% compare/2 takes a native Erlang
% array as well as a BSON-encoded
% array and returns a boolean
% indicating whether they are
% conceptually the same
compare([], <<0>>) ->
    true;
compare(_Query, <<0>>) ->
    false;
compare([], _Payload) ->
    false;
compare([Hd|Tl], Payload) ->
    {Datatype, _Key, _Prefix, Value, Remainder} = pop(Payload),
    case encode(Datatype, Hd) of
        Value ->
            compare(Tl, Remainder);
        _ ->
            false
    end.

