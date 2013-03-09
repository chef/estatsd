-module(estatsd_shp).

-export([parse_packet/1]).

-ifdef(TEST).
-compile([export_all]).
-endif.

-define(SHP_VERSION, 1).

-include("estatsd.hrl").


% @doc Parse a binary in Stats Hero Protocol Version 1
%
-spec parse_packet(binary()) ->
        {bad_version, binary()}
        | {bad_length, {[any()] | integer(), binary()}}
        | [#shp_metric{}].
parse_packet(<<"1|", Rest/binary>>) ->
    parse_packet(Rest, []);
parse_packet(Packet) when is_binary(Packet) ->
    {bad_version, Packet}.

parse_packet(<<"\n", Rest/binary>>, Acc) ->
    Acc1 = lists:reverse(Acc),
    Length = try
                 list_to_integer(Acc1)
             catch
                 error:badarg ->
                     Acc1
             end,
    Actual = size(Rest),
    case Length =:= Actual of
        true ->
            parse_body({Length, Rest});
        false ->
            {bad_length, {Length, Rest}}
    end;
parse_packet(<<C:8, Rest/binary>>, Acc) ->
    parse_packet(Rest, [C|Acc]);
parse_packet(<<>>, Acc) ->
    {bad_length, {lists:reverse(Acc), <<>>}}.

-spec parse_body({non_neg_integer(), binary()}) ->
    [(#shp_metric{} | {bad_metric, term()})].

parse_body({Length, GZBody = <<31, 139, _Rest/binary>>}) ->
    Body = zlib:gunzip(GZBody),
    parse_body({Length, Body});
parse_body({_Length, Body}) ->
    try
        Lines = binary:split(Body, <<"\n">>, [global]),
        [ parse_metric(L) || L <- Lines, L =/= <<>> ]
    catch
        error:Why ->
            error_logger:error_report({bad_metric,
                                       {Body, Why, erlang:get_stacktrace()}}),
            throw({bad_metric_body, Body})

    end.

-spec parse_metric(binary()) -> #shp_metric{}.

parse_metric(Bin) ->
    try
        [Key, Value, Type | Rate] = binary:split(Bin, [<<":">>, <<"|">>],
                                                 [global]),
        #shp_metric{key = Key, value = to_int(Value), type = parse_type(Type),
                    sample_rate = parse_sample_rate(Rate)}
    catch
        throw:{bad_metric, Why} ->
            {bad_metric, Why};
        error:{badmatch, _} ->
            {bad_metric, {parse_error, Bin}}
    end.

-spec parse_type(<<_:8, _:_*8>>) -> 'g' | 'h' | 'm' | 'mr'.
parse_type(<<"m">>) ->
    m;
parse_type(<<"h">>) ->
    h;
parse_type(<<"mr">>) ->
    mr;
parse_type(<<"g">>) ->
    g;
parse_type(Unknown) ->
    throw({bad_metric, {unknown_type, Unknown}}).

-spec parse_sample_rate([binary()]) -> float().

parse_sample_rate([]) ->
    undefined;
parse_sample_rate([<<"@", FloatBin/binary>>]) ->
    try
        list_to_float(binary_to_list(FloatBin))
    catch
        error:badarg ->
            throw({bad_metric, {bad_sample_rate, FloatBin}})
    end;
parse_sample_rate(L) ->
    throw({bad_metric, {bad_sample_rate, L}}).

-spec to_int(binary()) -> integer().

to_int(Value) when is_binary(Value) ->
    try
        list_to_integer(binary_to_list(Value))
    catch
        error:badarg ->
            throw({bad_metric, {bad_value, Value}})
    end.
