-module(bson).

-export([load/1, validate/1, parse/1, filter/2, safe_search/2, fast_search/2]).

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

safe_search(?BSON, KeyValues) ->
    KeyValuesBinary = [{list_to_binary(K), V} || {K,V} <- KeyValues],
    case match(safe, Payload, KeyValuesBinary) of
        true->
            ?BSON;
        false->
            ?EMPTY_BSON
    end.

fast_search(?BSON, KeyValues) ->
    KeyValuesParsed = [{list_to_binary(K), encode(V)} || {K,V} <- KeyValues],
    KeyValuesBinary = [{K, V} || {K,{_,_,V}} <- KeyValuesParsed],
    case match(fast, Payload, KeyValuesBinary) of
        true->
            ?BSON;
        false->
            ?EMPTY_BSON
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% PRIVATE FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_key(Data) ->
    get_key(Data,<<>>).
get_key(<<0, Remainder/binary>>, Key) ->
    {Key, Remainder};
get_key(<<Char, Remainder/binary>>, Key) ->
    get_key(Remainder, <<Key/binary, Char>>);
get_key(<<>>, _Key) ->
    {error, cant_get_key}.

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

get_value(?BSON_DOUBLE, Payload) ->
    chop(8, Payload);
get_value(?BSON_STRING, Payload) ->
    chop(exclusive, 32, Payload);
get_value(?BSON_DOCUMENT, Payload) ->
    chop(inclusive, 32, Payload);
get_value(?BSON_ARRAY, Payload) ->
    chop(inclusive, 32, Payload);
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
    {error, error, unrecognized_datatype}.

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
    CodeString = binary_to_list(Code),

    % Parse scope 
    {_, <<_:1/binary, C/binary>>, _} = chop(inclusive, 32, Scope),
    {Key, Value} = get_key(C),
    {<<Length:32/little-integer>>, D, _} = chop(exclusive, 32, Value),
    Size = Length -1,
    <<E:Size/binary, _/binary>> = D,

    % Concatenate Code and Scope
    CodeString ++ ",{\"" ++ binary_to_list(Key) ++ "\",\"" ++ binary_to_list(E) ++ "\"}";
decode(?BSON_INT32, <<Value:32/little-signed-integer>>) ->
    Value;
decode(?BSON_TS, <<_Increment:32, Seconds:32/little-integer>>) ->
    Seconds;
decode(?BSON_INT64, <<Value:64/little-signed-integer>>) ->
    Value;
decode(?BSON_MINKEY, _Value) ->
    minkey;
decode(?BSON_MAXKEY, _Value) ->
    maxkey;
decode(_, _Value) ->
    {error, corrupted_binary}.

encode(Value) when is_integer(Value) , abs(Value) < 2147483648 -> % 2^31
    {?BSON_INT32, <<>>, <<Value:32/little-signed-integer>>};
encode(Value) when is_integer(Value) ->
    {?BSON_INT64, <<>>, <<Value:64/little-signed-integer>>};
encode(Value) when is_float(Value) ->
    {?BSON_DOUBLE, <<>>, <<Value:64/little-signed-float>>};
encode(true) ->
    {?BSON_BOOL, <<>>,<<1>>};
encode(false) ->
    {?BSON_BOOL, <<>>,<<0>>};
encode(null) ->
    {?BSON_NULL, <<>>,<<>>};
encode(minkey) ->
    {?BSON_MINKEY, <<>>,<<>>};
encode(maxkey) ->
    {?BSON_MAXKEY, <<>>,<<>>};
encode({Key, Value}) ->
    {Type, Prefix, Data} = encode(Value),
    KeyBinary = list_to_binary(Key),
    Element = <<Type, KeyBinary/binary, 0, Prefix/binary, Data/binary>>,
    {ignore, <<>>, Element};
encode(Value=[{_,_}|_]) -> % Nested document
    Elements = [encode(X) || X <- Value],
    Payload = list_to_binary([ E || {_T,_P,E} <- Elements ]),
    Size = byte_size(Payload) + 5,
    {?BSON_DOCUMENT, <<Size:32/little-integer>>, <<Payload/binary, 0>>};
encode(Value) when is_binary(Value)->
    case byte_size(Value) of
        12 -> % ObjectId
            {?BSON_OBJECTID, <<>>, Value};
        _ -> % Binary
            Size = byte_size(Value),
            {?BSON_BINARY, <<Size:40/little-integer>>, Value}
    end;
encode(Value) when is_list(Value) ->
    case io_lib:printable_unicode_list(Value) of
        true -> % String
            BitString = list_to_binary(Value),
            Size = byte_size(BitString) + 1,
            {?BSON_STRING, <<Size:32/little-integer>>, <<BitString/binary,0>>};
        _ -> % Array
            Elements = [encode(X) || X <- Value],
            Payload = bson_array(Elements, <<>>, 0),
            Size = byte_size(Payload) + 1,
            {?BSON_ARRAY, <<Size:32/little-integer>>, <<Payload/binary, 0>>}
    end.

% [{Type, Prefix, Value}] -> <<Type, Index, 0, Prefix, Value, 0>>
bson_array([], Result, _Counter) ->
    Result;
bson_array([{Type,Prefix,Value}|T], Result, Counter) ->
    bson_array(T, <<Result/binary, Type, Counter, 0, Prefix/binary, Value/binary>>, Counter+1).


% Pop a single element from the Payload
pop(<<0>>) ->
    eod;
pop(<<Datatype, Payload/binary>>) ->
    {Key, Rest} = get_key(Payload),
    {Prefix, Value, Remainder} = get_value(Datatype, Rest),
    {Datatype, Key, Prefix, Value, Remainder}.

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

parse(<<0>>, List) ->
    List;
parse(Payload, List) ->
    {Datatype, Key, Prefix, Value, Remainder} = pop(Payload),
    parse(Remainder, [{binary_to_list(Key), decode(Datatype, <<Prefix/binary, Value/binary>>)} | List]).

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

match(_Mode, _Payload, []) ->
    true;
match(_Mode, <<0>>, _KeysValues) ->
    false;
match(safe, Payload, KeyValues) ->
    {Datatype, Key, Prefix, Value, Remainder} = pop(Payload),
    KeyValue = {Key, decode(Datatype, <<Prefix/binary, Value/binary>>)},
    case lists:member(KeyValue, KeyValues) of
        true ->
            match(safe, Remainder, lists:delete(KeyValue, KeyValues));
        false ->
            match(safe, Remainder, KeyValues)
    end;
match(fast, Payload, KeyValues) ->
    {_Datatype, Key, _Prefix, Value, Remainder} = pop(Payload),
    case lists:member({Key, Value}, KeyValues) of
        true ->
            match(fast, Remainder, lists:delete({Key, Value}, KeyValues));
        false ->
            match(fast, Remainder, KeyValues)
    end.

