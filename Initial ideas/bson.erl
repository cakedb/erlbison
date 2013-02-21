-module(bson).

-export([parse_file/1,
		 validate_file/1,
		 search_file/2]).

-define(BSON_EOD, <<0>>).
-define(BSON_DOUBLE,<<1>>).
-define(BSON_STRING,<<2>>).
-define(BSON_DOCUMENT,<<3>>).
-define(BSON_ARRAY,<<4>>).
-define(BSON_BINARY,<<5>>).
-define(BSON_OBJECTID,<<7>>).
%-define(BSON_TRUE,<<8,0>>).
%-define(BSON_FALSE,<<8,1>>).
-define(BSON_BOOL,<<8>>).
-define(BSON_DATETIME,<<9>>).
-define(BSON_NULL,<<16#A>>).
-define(BSON_REGEX,<<16#B>>).
-define(BSON_DBPOINTER,<<16#C>>).
-define(BSON_JSCODE,<<16#D>>).
-define(BSON_SYMBOL,<<16#E>>).
-define(BSON_JSCODEWS,<<16#F>>).
-define(BSON_INT32,<<16#10>>).
-define(BSON_TS,<<16#11>>).
-define(BSON_INT64,<<16#12>>).
-define(BSON_MINKEY,<<16#FF>>).
-define(BSON_MAXKEY,<<16#7F>>).



get_key(Data) ->
	get_key(Data,<<>>).
get_key(<<0,TheRest/binary>>,Key) ->
	{Key,TheRest};
get_key(<<C:8,TheRest/binary>>,Key) ->
	get_key(<<TheRest/binary>>,<<Key/binary,C>>).

get_string(Data) ->
	% Get Key
	{Key,TheRest} = get_key(Data),
	<<Length:32/little-integer,String:Length/binary,TheRest2/binary>> = TheRest,
	{{?BSON_STRING,Key,String},TheRest2}.

parse_file(FileName) ->
	{ok,Data} = file:read_file(FileName),
	io:format("~p~n",[Data]),
	parse(Data).

parse(Data) ->
	<<Length:32/little-integer,TheRest/binary>> = Data,
	io:format("Doc Length: ~p~n",[Length]),
    parse_more(TheRest,"",0).

parse_more(<<0>>,_,_) ->
	io:format("End of Document~n");

parse_more(Data,Prefix,Depth) ->
	{Type,Data2} = get_type(Data),
	case Type of
		?BSON_EOD    -> io:format("EOD Marker~n"),
						parse_more(Data2,Prefix,Depth - 1);

		?BSON_DOUBLE  -> io:format("Double:~n"),
						{{Key,Double},Data3} = get_double(Data2),
						print_key(Key,Prefix,Depth),
						io:format("Double:~p~n",[Double]),
						parse_more(Data3,Prefix,Depth);


		?BSON_BOOL -> io:format("Bool:~n"),
						{{Key,Bool},Data3} = get_bool(Data2),
						print_key(Key,Prefix,Depth),
						io:format("Bool:~p~n",[Bool]),
						parse_more(Data3,Prefix,Depth);



		?BSON_STRING -> io:format("String:~n"),
						{{Key,String},Data3} = get_string(Data2),
						print_key(Key,Prefix,Depth),
						io:format("String:~p~n",[remove_trailing_null(String)]),
						parse_more(Data3,Prefix,Depth);

		?BSON_DOCUMENT-> io:format("Document:~n"),
						{Key,Length,Data3} = get_document(Data2),
						print_key(Key,Prefix,Depth),
						io:format("Length:~p~n",[Length]),
						parse_more(Data3,Prefix,Depth + 1);

		?BSON_ARRAY  -> io:format("Array:~n"),
						{Key,Length,Data3} = get_array(Data2),
						print_key(Key,Prefix,Depth),
						io:format("Length:~p~n",[Length]),
						parse_more(Data3,Prefix,Depth + 1);
		?BSON_INT32  -> io:format("Int32:~n"),
						{{Key,Int32},Data3} = get_int32(Data2),
						print_key(Key,Prefix,Depth),
						io:format("Int32:~p~n",[Int32]),
						parse_more(Data3,Prefix,Depth);

		?BSON_INT64  -> io:format("Int64:~n"),
						{{Key,Int64},Data3} = get_int64(Data2),
						print_key(Key,Prefix,Depth),
						io:format("Int64:~p~n",[Int64]),
						parse_more(Data3,Prefix,Depth);

		UnknownType	 -> io:format("Can't parse type '~p'~n",[UnknownType])
	end.


print_key(Key,Prefix,Depth) ->
	case Prefix of
		"" -> Prefix2 = "";
		_  -> Prefix2 = Prefix ++ "."
	end,

    

	FinalKey = gen_depth(Depth) ++ Prefix2 ++ binary_to_list(Key),
	io:format("Key: ~s~n",[FinalKey]).



get_int32(Data) ->
	{Key,TheRest} = get_key(Data),
	<<Int32:32/little-integer,TheRest2/binary>> = TheRest,
	{{?BSON_INT32,Key,Int32},TheRest2}.

get_bool(Data) ->
	{Key,TheRest} = get_key(Data),
	<<Bool:1/binary,TheRest2/binary>> = TheRest,
	case Bool of
		<<0>> ->
			{{?BSON_BOOL, Key,true},TheRest2};
		<<1>> ->
			{{?BSON_BOOL, Key,false},TheRest2}
	end.


get_int64(Data) ->
	{Key,TheRest} = get_key(Data),
	<<Int64:64/little-integer,TheRest2/binary>> = TheRest,
	{{?BSON_INT64, Key,Int64},TheRest2}.

get_double(Data) ->
	{Key,TheRest} = get_key(Data),
	<<Double:64/little-signed-float,TheRest2/binary>> = TheRest,
	{{?BSON_DOUBLE, Key,Double},TheRest2}.
get_document(Data) ->
	{Key,Data2} = get_key(Data),
	{Length,Data3} = get_length(Data2),
	{{?BSON_DOCUMENT, Key,Length},Data3}.

get_array(Data) ->
	{Key,Data2} = get_key(Data),
	{Length,Data3} = get_length(Data2),
	{{?BSON_ARRAY, Key,Length},Data3}.

get_type(Data) ->
	<<Type:1/binary,TheRest/binary>> = Data,
	{Type,TheRest}.

get_length(Data) ->
	<<Length:32/little-integer,TheRest/binary>> = Data,
	{Length,TheRest}.


remove_trailing_null(Data) ->
	Length = size(Data) - 1,
	<<Printable:Length/binary,0>> = Data,
	Printable.


gen_depth(Depth) ->
	gen_depth([],Depth).

gen_depth(List,0) ->
	List;
gen_depth(List,Depth) ->
	gen_depth(List ++ "    ",Depth -1).



validate_file(FileName) ->
	{ok,Data} = file:read_file(FileName),
	validate_bson(Data).



validate_bson(Data) ->
	<<_Length:32/little-integer,TheRest/binary>> = Data,
    validate_bson(TheRest,"").

%validate_bson(<<0>>,_) ->
%	io:format("Document Valid.~n");

validate_bson(Data,"") ->
	case get_next(Data) of
		{Value,TheRest} -> validate_bson(TheRest,""),
						   io:format("~p~n",[Value]);
		eod             -> io:format("Document Valid.~n");
		bad_data        -> io:format("Document Invalid!~n")
	end.





get_next(<<0>>) ->
	eod;
get_next(Data) ->
	{Type,Data2} = get_type(Data),
	case Type of
		?BSON_EOD    -> {{bson_eod,na,na},Data2};
		?BSON_DOUBLE  ->get_double(Data2);
		?BSON_BOOL -> get_bool(Data2);
		?BSON_STRING -> get_string(Data2);
		?BSON_DOCUMENT-> get_document(Data2);
		?BSON_ARRAY  ->  get_array(Data2);		
		?BSON_INT32  -> get_int32(Data2);
		?BSON_INT64  -> get_int64(Data2);
		UnknownType  -> io:format("UnknownType: ~p~n",[UnknownType]),
						bad_data
	end.



search_file(FileName,FindKey) ->
	{ok,Data} = file:read_file(FileName),
	<<_Length:32/little-integer,TheRest/binary>> = Data,
	search_bson(TheRest,list_to_binary(FindKey)).



search_bson(Data,FindKey) ->
	case get_next(Data) of
		{{Type,Key,Value},TheRest} -> case Key == FindKey of
										true -> io:format("Found!~n");
										false -> search_bson(TheRest,FindKey)
									end;
		eod             -> io:format("Not Found.~n");
		bad_data        -> io:format("Document Invalid!~n")
	end.








