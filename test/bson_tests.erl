-module(bson_tests).

-include_lib("eunit/include/eunit.hrl").

-define(EMPTY_BSON, <<5,0,0,0,0>>).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% FIXTURES
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start() ->
    io:format(user,"~nInitiating tests...~n",[]),
    ok.
 
stop(_) ->
    ok.
 
instantiator(_) ->
    Query1 = bson:validate(<<32,0,0,34,34,23,23,43,0>>),
    Query2 = bson:validate(<<5,0,0,0,0>>),
    Query3 = bson:parse(<<5,0,0,0,0>>),
    Bson = bson:load("test/test.bson"),
    Query4 = bson:validate(Bson),

    Query5 = bson:filter(Bson, ["some_double"]),
    Query6 = bson:validate(Query5),
    Query7 = bson:parse(Query5),

    Query8 = bson:filter(Bson, ["some_string"]),
    Query9 = bson:validate(Query8),
    Query10 = bson:parse(Query8),

    Query11 = bson:filter(Bson, ["some_document"]),
    Query12 = bson:validate(Query11),
    Query13 = bson:parse(Query11),

    Query14 = bson:filter(Bson, ["some_array"]),
    Query15 = bson:validate(Query14),
    Query16 = bson:parse(Query14),

    Query17 = bson:filter(Bson, ["some_binary"]),
    Query18 = bson:validate(Query17),
    Query19 = bson:parse(Query17),

    Query20 = bson:filter(Bson, ["some_ObjectId"]),
    Query21 = bson:validate(Query20),
    Query22 = bson:parse(Query20),

    Query23 = bson:filter(Bson, ["some_bool"]),
    Query24 = bson:validate(Query23),
    Query25 = bson:parse(Query23),

    Query26 = bson:filter(Bson, ["some_datetime"]),
    Query27 = bson:validate(Query26),
    Query28 = bson:parse(Query26),

    Query29 = bson:filter(Bson, ["some_null"]),
    Query30 = bson:validate(Query29),
    Query31 = bson:parse(Query29),

    Query32 = bson:filter(Bson, ["some_regex"]),
    Query33 = bson:validate(Query32),
    Query34 = bson:parse(Query32),

    Query35 = bson:filter(Bson, ["some_other_regex"]),
    Query36 = bson:validate(Query35),
    Query37 = bson:parse(Query35),

    Query38 = bson:filter(Bson, ["some_jscode"]),
    Query39 = bson:validate(Query38),
    Query40 = bson:parse(Query38),

    Query41 = bson:filter(Bson, ["some_jscodews"]),
    Query42 = bson:validate(Query41),
    Query43 = bson:parse(Query41),

    Query44 = bson:filter(Bson, ["some_int32"]),
    Query45 = bson:validate(Query44),
    Query46 = bson:parse(Query44),

    Query47 = bson:filter(Bson, ["some_timestamp"]),
    Query48 = bson:validate(Query47),
    Query49 = bson:parse(Query47),

    Query50 = bson:filter(Bson, ["some_int64"]),
    Query51 = bson:validate(Query50),
    Query52 = bson:parse(Query50),

    Query53 = bson:filter(Bson, ["some_other_int64"]),
    Query54 = bson:validate(Query53),
    Query55 = bson:parse(Query53),

    Query56 = bson:filter(Bson, ["some_minkey"]),
    Query57 = bson:validate(Query56),
    Query58 = bson:parse(Query56),

    Query59 = bson:filter(Bson, ["some_maxkey"]),
    Query60 = bson:validate(Query59),
    Query61 = bson:parse(Query59),

    Query62 = bson:filter(Bson, ["some_int32", "some_double"]),
    Query63 = bson:validate(Query62),
    Query64 = bson:parse(Query62),

    Query65 = bson:filter(Bson, ["some_int32", "some_garbage"]),
    Query66 = bson:validate(Query65),
    Query67 = bson:parse(Query65),

    Query68 = bson:filter(Bson, []),
    Query69 = bson:validate(Query68),
    Query70 = bson:parse(Query68),

    % No match
    Query71 = bson:safe_search(Bson, [{"some_int",1}, {"some_double",-233.22}]),
    Query72 = bson:safe_search(Bson, [{"some_garbage",1}]),
    Query73 = bson:safe_search(Bson, [{"some_int",3}]),

    Query74 = bson:fast_search(Bson, [{"some_int",1}, {"some_double",-233.22}]),
    Query75 = bson:fast_search(Bson, [{"some_garbage",1}]),
    Query76 = bson:fast_search(Bson, [{"some_int",3}]),

    % Matches
    Query77 = bson:safe_search(Bson, []),
    Query78 = bson:safe_search(Bson, [{"some_double" , 87363.343425}]),
    Query79 = bson:safe_search(Bson, [{"some_string" , "HelloWorld"}]),
    Query80 = bson:safe_search(Bson, [{"some_document" , [{"nested","document"}]}]),
    Query81 = bson:safe_search(Bson, [{"some_array" , [1, true, "a", null]}]),
    Query82 = bson:safe_search(Bson, [{"some_binary" , <<"HelloWorld">>}]),
    Query83 = bson:safe_search(Bson, [{"some_ObjectId" , <<(16#5130d8c37603e11f843f9c05):12/unit:8>>}]),
    Query84 = bson:safe_search(Bson, [{"some_bool" , true}]),
    Query85 = bson:safe_search(Bson, [{"some_datetime" , 1362141030000}]),
    Query86 = bson:safe_search(Bson, [{"some_null" , null}]),
    Query87 = bson:safe_search(Bson, [{"some_regex" , "a*b?c\\?"}]),
    Query88 = bson:safe_search(Bson, [{"some_other_regex" , "(?:\\+?1\\s*(?:[.-]\\s*)?)"}]),
    Query89 = bson:safe_search(Bson, [{"some_jscode" , "document.write(\"Hello World!\")"}]),
    Query90 = bson:safe_search(Bson, [{"some_jscodews" , "document.write(\"Hello World!\"),{\"scopevar\",\"scopevalue\"}"}]),
    Query91 = bson:safe_search(Bson, [{"some_int32" , 1}]),
    Query92 = bson:safe_search(Bson, [{"some_timestamp" , 1361851945}]),
    Query93 = bson:safe_search(Bson, [{"some_int64" , 3000000000}]),
    Query94 = bson:safe_search(Bson, [{"some_other_int64" , -20}]),
    Query95 = bson:safe_search(Bson, [{"some_minkey" , minkey}]),
    Query96 = bson:safe_search(Bson, [{"some_maxkey" , maxkey}]),
    Query97 = bson:safe_search(Bson, [{"some_int32" , 1}, {"some_int64", 3000000000}]),

    Query98 = bson:fast_search(Bson, []),
    Query99 = bson:fast_search(Bson, [{"some_double" , 87363.343425}]),
    Query100 = bson:fast_search(Bson, [{"some_string" , "HelloWorld"}]),
    Query101 = bson:fast_search(Bson, [{"some_document" , [{"nested","document"}]}]),

    % fast_search doesn't work as of now on arrays because of bson's
    % arbitrary explicit indexing
%    Query102 = bson:fast_search(Bson, [{"some_array" , [1, true, "a", null]}]),

    Query103 = bson:fast_search(Bson, [{"some_binary" , <<"HelloWorld">>}]),
    Query104 = bson:fast_search(Bson, [{"some_ObjectId" , <<(16#5130d8c37603e11f843f9c05):12/unit:8>>}]),
    Query105 = bson:fast_search(Bson, [{"some_bool" , true}]),

    % Datetime unsupported in fast_search
%    Query106 = bson:fast_search(Bson, [{"some_datetime" , 1362141030000}]),

    Query107 = bson:fast_search(Bson, [{"some_null" , null}]),
    
    % Regex unsupported in fast_search
%    Query108 = bson:fast_search(Bson, [{"some_regex" , "a*b?c\\?"}]),
%    Query109 = bson:fast_search(Bson, [{"some_other_regex" , "(?:\\+?1\\s*(?:[.-]\\s*)?)"}]),

    % Javascript code unsupported in fast_search
%    Query110 = bson:fast_search(Bson, [{"some_jscode" , "document.write(\"Hello World!\")"}]),
%    Query111 = bson:fast_search(Bson, [{"some_jscodews" , "document.write(\"Hello World!\"),{\"scopevar\",\"scopevalue\"}"}]),

    Query112 = bson:fast_search(Bson, [{"some_int32" , 1}]),

    % timestamp unsupported in fast_search
%    Query113 = bson:fast_search(Bson, [{"some_timestamp" , 1361851945}]),

    Query114 = bson:fast_search(Bson, [{"some_int64" , 3000000000}]),

    % Can't know if 32 bits or 64 bits int...
%    Query115 = bson:fast_search(Bson, [{"some_other_int64" , -20}]),

    Query116 = bson:fast_search(Bson, [{"some_minkey" , minkey}]),
    Query117 = bson:fast_search(Bson, [{"some_maxkey" , maxkey}]),
    Query118 = bson:fast_search(Bson, [{"some_int32" , 1}, {"some_int64", 3000000000}]),


    [
        ?_assertEqual(Query1, false),
        ?_assertEqual(Query2, true),
        ?_assertEqual(Query3, []),
        ?_assertEqual(Query4, true),

        ?_assertEqual(Query6, true),
        ?_assertEqual(Query7, [{"some_double", 87363.343425}]),

        ?_assertEqual(Query9, true),
        ?_assertEqual(Query10, [{"some_string", "HelloWorld"}]),

        ?_assertEqual(Query12, true),
        ?_assertEqual(Query13, [{"some_document", [{"nested","document"}]}]),

        ?_assertEqual(Query15, true),
        ?_assertEqual(Query16, [{"some_array", [1,true,"a",null]}]),

        ?_assertEqual(Query18, true),
        ?_assertEqual(Query19, [{"some_binary", <<"HelloWorld">>}]),

        ?_assertEqual(Query21, true),
        ?_assertMatch(Query22, [{"some_ObjectId", <<81,48,216,195,118,3,225,31,132,63,156,5>>}]),

        ?_assertEqual(Query24, true),
        ?_assertEqual(Query25, [{"some_bool", true}]),

        ?_assertEqual(Query27, true),
        ?_assertEqual(Query28, [{"some_datetime", 1362141030000}]),

        ?_assertEqual(Query30, true),
        ?_assertEqual(Query31, [{"some_null", null}]),

        ?_assertEqual(Query33, true),
        ?_assertEqual(Query34, [{"some_regex", "a*b?c\\?"}]),

        ?_assertEqual(Query36, true),
        ?_assertEqual(Query37, [{"some_other_regex", "(?:\\+?1\\s*(?:[.-]\\s*)?)"}]),

        ?_assertEqual(Query39, true),
        ?_assertEqual(Query40, [{"some_jscode", "document.write(\"Hello World!\")"}]),

        ?_assertEqual(Query42, true),
        ?_assertEqual(Query43, [{"some_jscodews", "document.write(\"Hello World!\"),{\"scopevar\",\"scopevalue\"}"}]),

        ?_assertEqual(Query45, true),
        ?_assertEqual(Query46, [{"some_int32", 1}]),

        ?_assertEqual(Query48, true),
        ?_assertEqual(Query49, [{"some_timestamp", 1361851945}]),

        ?_assertEqual(Query51, true),
        ?_assertEqual(Query52, [{"some_int64", 3000000000}]),

        ?_assertEqual(Query54, true),
        ?_assertEqual(Query55, [{"some_other_int64", -20}]),

        ?_assertEqual(Query57, true),
        ?_assertEqual(Query58, [{"some_minkey", minkey}]),

        ?_assertEqual(Query60, true),
        ?_assertEqual(Query61, [{"some_maxkey", maxkey}]),

        ?_assertEqual(Query63, true),
        ?_assertEqual(Query64, [{"some_int32", 1},{"some_double", 87363.343425}]),

        ?_assertEqual(Query66, true),
        ?_assertEqual(Query67, [{"some_int32", 1}]),

        ?_assertEqual(Query69, true),
        ?_assertEqual(Query70, []),

        % No match - safe_search
        ?_assertEqual(Query71, ?EMPTY_BSON),
        ?_assertEqual(Query72, ?EMPTY_BSON),
        ?_assertEqual(Query73, ?EMPTY_BSON),
        
        % No match - fast_search
        ?_assertEqual(Query74, ?EMPTY_BSON),
        ?_assertEqual(Query75, ?EMPTY_BSON),
        ?_assertEqual(Query76, ?EMPTY_BSON),

        % Matches - safe_search
        ?_assertEqual(Query77, Bson),
        ?_assertEqual(Query78, Bson),
        ?_assertEqual(Query79, Bson),
        ?_assertEqual(Query80, Bson),
        ?_assertEqual(Query81, Bson),
        ?_assertEqual(Query82, Bson),
        ?_assertEqual(Query83, Bson),
        ?_assertEqual(Query84, Bson),
        ?_assertEqual(Query85, Bson),
        ?_assertEqual(Query86, Bson),
        ?_assertEqual(Query87, Bson),
        ?_assertEqual(Query88, Bson),
        ?_assertEqual(Query89, Bson),
        ?_assertEqual(Query90, Bson),
        ?_assertEqual(Query91, Bson),
        ?_assertEqual(Query92, Bson),
        ?_assertEqual(Query93, Bson),
        ?_assertEqual(Query94, Bson),
        ?_assertEqual(Query95, Bson),
        ?_assertEqual(Query96, Bson),
        ?_assertEqual(Query97, Bson),
        
        % Matches - fast_search
        ?_assertEqual(Query98, Bson),
        ?_assertEqual(Query99, Bson),
        ?_assertEqual(Query100, Bson),
        ?_assertEqual(Query101, Bson),

        % fast_search doesn't work as of now on arrays because of bson's
        % arbitrary explicit indexing
%        ?_assertEqual(Query102, Bson)%,

        ?_assertEqual(Query103, Bson),
        ?_assertEqual(Query104, Bson),
        ?_assertEqual(Query105, Bson),
        ?_assertEqual(Query107, Bson),
        ?_assertEqual(Query112, Bson),
        ?_assertEqual(Query114, Bson),
        ?_assertEqual(Query116, Bson),
        ?_assertEqual(Query117, Bson),
        ?_assertEqual(Query118, Bson)
    ].
        
cakedb_driver_test_() ->
    {setup,
     fun start/0,
     fun stop/1,
     fun instantiator/1}.

