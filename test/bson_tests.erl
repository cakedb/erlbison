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
    Query2 = bson:validate(?EMPTY_BSON),
    Query3 = bson:parse(?EMPTY_BSON),
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

    Query11b = bson:filter(Bson, ["some_nested_document"]),
    Query12b = bson:validate(Query11b),
    Query13b = bson:parse(Query11b),

    Query11c = bson:filter(Bson, ["some_other_document"]),
    Query12c = bson:validate(Query11c),
    Query13c = bson:parse(Query11c),

    Query14 = bson:filter(Bson, ["some_array"]),
    Query15 = bson:validate(Query14),
    Query16 = bson:parse(Query14),

    Query14b = bson:filter(Bson, ["some_other_array"]),
    Query15b = bson:validate(Query14b),
    Query16b = bson:parse(Query14b),

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

    Query26b = bson:filter(Bson, ["some_other_datetime"]),
    Query27b = bson:validate(Query26b),
    Query28b = bson:parse(Query26b),

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

    Query47b = bson:filter(Bson, ["some_other_timestamp"]),
    Query48b = bson:validate(Query47b),
    Query49b = bson:parse(Query47b),

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

    % Search - No match
    Query71 = bson:search(Bson, [{"some_int",1}, {"some_double",-233.22}]),
    Query72 = bson:search(Bson, [{"some_garbage",1}]),
    Query73 = bson:search(Bson, [{"some_int",3}]),
    Shorter = bson:search(Bson, [{"some_other_array",[0,1,2,3,4,5,6,7,8,9,10,11]}]),
    Longer = bson:search(Bson, [{"some_other_array",[0,1,2,3,4,5,6,7,8,9,10,11,12,13]}]),
    Query98 = bson:search(Bson, [{"some_int",fun(X)->X<0 end},{"some_double",fun(X)->X==-233.22 end}]),
    Query99 = bson:search(Bson, [{"some_garbage",fun(_X)->true end}]),
    Query100 = bson:search(Bson, [{"some_int",fun(X)->X rem 2 == 0 end}]),
    WrongType1 = bson:search(Bson, [{"some_string" , true}]), 
    WrongType2 = bson:search(Bson, [{"some_string" , -132.23}]), 

    % Search - Matches
    Query77 = bson:search(Bson, []),
    Query78 = bson:search(Bson, [{"some_double" , 87363.343425}]),
    Query79 = bson:search(Bson, [{"some_string" , "HelloWorld"}]),
    Query80 = bson:search(Bson, [{"some_document" , [{"nested","document"}]}]),
    Query80b = bson:search(Bson, [{"some_nested_document" , [{"doc1",[{"doc2","stuff"}]}]}]),
    Query80c = bson:search(Bson, [{"some_other_document" , [{"b",2},{"a",1}]}]),
    Query80d = bson:search(Bson, [{"some_other_document" , [{"a",1},{"b",2}]}]),
    Query81 = bson:search(Bson, [{"some_array" , [1, true, "a", null]}]),
    Query81b = bson:search(Bson, [{"some_other_array" , [0,1,2,3,4,5,6,7,8,9,10,11,12]}]),
    Query82 = bson:search(Bson, [{"some_binary" , <<"HelloWorld">>}]),
    Query83 = bson:search(Bson, [{"some_ObjectId" , <<(16#5130d8c37603e11f843f9c05):12/unit:8>>}]),
    Query84 = bson:search(Bson, [{"some_bool" , true}]),
    Query85 = bson:search(Bson, [{"some_datetime" , 1362141030000}]),
    Query86 = bson:search(Bson, [{"some_null" , null}]),
    Query87 = bson:search(Bson, [{"some_regex" , "a*b?c\\?"}]),
    Query88 = bson:search(Bson, [{"some_other_regex" , "(?:\\+?1\\s*(?:[.-]\\s*)?)"}]),
    Query89 = bson:search(Bson, [{"some_jscode" , "document.write(\"Hello World!\")"}]),
    Query90 = bson:search(Bson, [{"some_jscodews" , {"document.write(\"Hello World!\")","scopeID","scopevalue"}}]),
    Query91 = bson:search(Bson, [{"some_int32" , 1}]),
    Query92 = bson:search(Bson, [{"some_timestamp" , {0,1361851945}}]),
    Query93 = bson:search(Bson, [{"some_int64" , 3000000000}]),
    Query94 = bson:search(Bson, [{"some_other_int64" , -20}]),
    Query95 = bson:search(Bson, [{"some_minkey" , minkey}]),
    Query96 = bson:search(Bson, [{"some_maxkey" , maxkey}]),
    Query97 = bson:search(Bson, [{"some_int32" , 1}, {"some_int64", 3000000000}]),

    Query101 = bson:search(Bson, [{"some_double" , fun(X)->X>10000 end}]),

    Query102 = bson:search(Bson, [{"some_string" , fun(X)->string:len(X)==10 end}]),

    Query103 = bson:search(Bson, [{"some_document" , fun(X)->proplists:is_defined("nested",X) end}]),

    Query104 = bson:search(Bson, [{"some_array" , fun(X) -> lists:nth(1, X) =< 1.5 end}]),

    Query105 = bson:search(Bson, [{"some_other_datetime" , fun(X)->X>557460184000 end}]),

    Query106 = bson:search(Bson, [{"some_int32" , fun(X)->X<2 end}]),
    Query107 = bson:search(Bson, [{"some_other_array" , fun(X)->lists:nth(9, X) rem 2 == 0 end}]),
    Query108 = bson:search(Bson, [{"some_bool" , fun(X)->X end}, {"some_int64", 3000000000}]),

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

        ?_assertEqual(Query12b, true),
        ?_assertEqual(Query13b, [{"some_nested_document", [{"doc1",[{"doc2","stuff"}]}]}]),

        ?_assertEqual(Query12c, true),
        ?_assertEqual(Query13c, [{"some_other_document", [{"b",2},{"a",1}]}]),

        ?_assertEqual(Query15, true),
        ?_assertEqual(Query16, [{"some_array", [1,true,"a",null]}]),

        ?_assertEqual(Query15b, true),
        ?_assertEqual(Query16b, [{"some_other_array", [0,1,2,3,4,5,6,7,8,9,10,11,12]}]),

        ?_assertEqual(Query18, true),
        ?_assertEqual(Query19, [{"some_binary", <<"HelloWorld">>}]),

        ?_assertEqual(Query21, true),
        ?_assertMatch(Query22, [{"some_ObjectId", <<81,48,216,195,118,3,225,31,132,63,156,5>>}]),

        ?_assertEqual(Query24, true),
        ?_assertEqual(Query25, [{"some_bool", true}]),

        ?_assertEqual(Query27, true),
        ?_assertEqual(Query28, [{"some_datetime", 1362141030000}]),

        ?_assertEqual(Query27b, true),
        ?_assertEqual(Query28b, [{"some_other_datetime", 557460184123}]),

        ?_assertEqual(Query30, true),
        ?_assertEqual(Query31, [{"some_null", null}]),

        ?_assertEqual(Query33, true),
        ?_assertEqual(Query34, [{"some_regex", "a*b?c\\?"}]),

        ?_assertEqual(Query36, true),
        ?_assertEqual(Query37, [{"some_other_regex", "(?:\\+?1\\s*(?:[.-]\\s*)?)"}]),

        ?_assertEqual(Query39, true),
        ?_assertEqual(Query40, [{"some_jscode", "document.write(\"Hello World!\")"}]),

        ?_assertEqual(Query42, true),
        ?_assertEqual(Query43, [{"some_jscodews", {"document.write(\"Hello World!\")","scopeID","scopevalue"}}]),

        ?_assertEqual(Query45, true),
        ?_assertEqual(Query46, [{"some_int32", 1}]),

        ?_assertEqual(Query48, true),
        ?_assertEqual(Query49, [{"some_timestamp", {0, 1361851945}}]),

        ?_assertEqual(Query48b, true),
        ?_assertEqual(Query49b, [{"some_other_timestamp", {123456, 1361851945}}]),

        ?_assertEqual(Query51, true),
        ?_assertEqual(Query52, [{"some_int64", 3000000000}]),

        ?_assertEqual(Query54, true),
        ?_assertEqual(Query55, [{"some_other_int64", -20}]),

        ?_assertEqual(Query57, true),
        ?_assertEqual(Query58, [{"some_minkey", minkey}]),

        ?_assertEqual(Query60, true),
        ?_assertEqual(Query61, [{"some_maxkey", maxkey}]),

        ?_assertEqual(Query63, true),
        ?_assertEqual(lists:sort(Query64), lists:sort([{"some_int32", 1},{"some_double", 87363.343425}])),

        ?_assertEqual(Query66, true),
        ?_assertEqual(Query67, [{"some_int32", 1}]),

        ?_assertEqual(Query69, true),
        ?_assertEqual(Query70, []),

        % No match - search
        ?_assertEqual(Query71, ?EMPTY_BSON),
        ?_assertEqual(Query72, ?EMPTY_BSON),
        ?_assertEqual(Query73, ?EMPTY_BSON),
        ?_assertEqual(Shorter, ?EMPTY_BSON),
        ?_assertEqual(Longer, ?EMPTY_BSON),
        ?_assertEqual(Query98, ?EMPTY_BSON),
        ?_assertEqual(Query99, ?EMPTY_BSON),
        ?_assertEqual(Query100, ?EMPTY_BSON),
        ?_assertEqual(WrongType1, ?EMPTY_BSON),
        ?_assertEqual(WrongType2, ?EMPTY_BSON),
        
        % Matches - search
        ?_assertEqual(Query77, Bson),
        ?_assertEqual(Query78, Bson),
        ?_assertEqual(Query79, Bson),
        ?_assertEqual(Query80, Bson),
        ?_assertEqual(Query80b, Bson),
        ?_assertEqual(Query80c, Bson),
        ?_assertEqual(Query80d, Bson),
        ?_assertEqual(Query81, Bson),
        ?_assertEqual(Query81b, Bson),
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

        ?_assertEqual(Query101, Bson),
        ?_assertEqual(Query102, Bson),
        ?_assertEqual(Query103, Bson),
        ?_assertEqual(Query104, Bson),
        ?_assertEqual(Query105, Bson),
        ?_assertEqual(Query106, Bson),
        ?_assertEqual(Query107, Bson),
        ?_assertEqual(Query108, Bson)
    ].
        
cakedb_driver_test_() ->
    {setup,
     fun start/0,
     fun stop/1,
     fun instantiator/1}.

