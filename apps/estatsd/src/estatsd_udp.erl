-module(estatsd_udp).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-define(to_int(Value), list_to_integer(binary_to_list(Value))).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link(?MODULE, [], []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
-record(state, {port                  :: non_neg_integer(),
                socket                :: inet:socket(),
                batch = dict:new()    :: dict()
               }).

init([]) ->
    {ok, Port} = application:get_env(estatsd, udp_listen_port),
    {ok, RecBuf} = application:get_env(estatsd, udp_recbuf),
    {ok, BatchMax} = application:get_env(estatsd, udp_max_batch_size),
    {ok, BatchAge} = application:get_env(estatsd, udp_max_batch_age),
    error_logger:info_msg("estatsd will listen on UDP ~p with recbuf ~p~n",
                          [Port, RecBuf]),
    error_logger:info_msg("batch size ~p with max age of ~pms~n",
                          [BatchMax, BatchAge]),
    {ok, Socket} = gen_udp:open(Port, [binary, {active, once},
                                       {recbuf, RecBuf}]),
    timer:send_interval(BatchAge, dump_stats),
    {ok, #state{port = Port, socket = Socket}}.

handle_call(_Request, _From, State) ->
    {noreply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info({udp, Socket, _Host, _Port, Bin}, #state{batch=Batch}=State) ->
    inet:setopts(Socket, [{active, once}]),
    case parse_packet(Bin) of
        skip ->
            {noreply, State};
        {Name, Type, Stat} ->
            Batch1 = store_stat(Batch, Name, Type, Stat),
            {noreply, State#state{batch=Batch1}}
    end;
handle_info(dump_stats, #state{batch=Batch}=State) ->
    case dict:size(Batch) of
        0 ->
            {noreply, State};
        _ ->
            error_logger:info_msg("spawn batch ~p TIMEOUT~n", [dict:size(Batch)]),
            start_batch_worker(Batch),
            {noreply, State#state{batch=dict:new()}}
    end;
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
start_batch_worker(Batch) ->
    %% Make sure we process messages in the order received
    proc_lib:spawn(fun() -> handle_messages(dict:to_list(Batch), 0) end).

handle_messages([], Count) ->
    io:format("Sent ~p messages for batch~n", [Count]),
    ok;
handle_messages([{{Name, _Type}, Values}|T], Count) ->
    folsom_metrics:notify({Name, Values}),
    handle_messages(T, Count + 1).

parse_packet(<<>>) ->
    skip;
parse_packet(Bin) ->
    case binary:split(Bin, [<<":">>, <<"|">>], [global]) of
        [Key, Value, Type] ->
            {ok, _} = estatsd_folsom:ensure_metric(Key, Type),
            {Key, Type, convert_value(Type, Value)};
        _ ->
            skip
    end.

store_stat(Batch, Name, Type, Stat) ->
    Key = {Name, Type},
    case dict:find(Key, Batch) of
        error ->
            dict:store(Key, [Stat], Batch);
        {ok, Stat0} ->
            dict:store(Key, [Stat|Stat0], Batch)
    end.

convert_value(<<"e">>, Value) ->
    Value;
convert_value(Type, Value) ->
    Value1 = ?to_int(Value),
    case Type of
        <<"d">> ->
            {dec, Value1};
        <<"c">> ->
            {inc, Value1};
        _ ->
            Value1
    end.

parse_line(<<>>) ->
    skip;
parse_line(Bin) ->
    [Key, Value, Type] = binary:split(Bin, [<<":">>, <<"|">>], [global]),
    {ok, _} = estatsd_folsom:ensure_metric(Key, Type),
    send_metric(Type, Key, Value),
    ok.

send_metric(Type, Key, Value) when Type =:= <<"d">> orelse Type =:= <<"c">> ->
    {EstatsFun, FolsomTag} = case Type of
                                 <<"d">> -> {decrement, dec};
                                 <<"c">> -> {increment, inc}
                             end,
    IntValue = ?to_int(Value),
    estatsd:EstatsFun(Key, IntValue),
    folsom_metrics:notify({Key, {FolsomTag, IntValue}});
send_metric(<<"ms">>, Key, Value) ->
    IntValue = ?to_int(Value),
    estatsd:timing(Key, IntValue),
    folsom_metrics:notify({Key, IntValue});
send_metric(<<"e">>, Key, Value) ->
    folsom_metrics:notify({Key, Value});
send_metric(_Type, Key, Value) ->
    IntValue = ?to_int(Value),
    folsom_metrics:notify({Key, IntValue}).

% TODO:
        % <<"mr">> ->
        %     % meter reader (expects monotonically increasing values)
        %     erlang:error({not_implemented, <<"mr">>});
