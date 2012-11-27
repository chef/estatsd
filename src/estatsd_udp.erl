-module(estatsd_udp).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-define(to_int(Value), list_to_integer(binary_to_list(Value))).

-include("estatsd.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
         start_link/0,
         what_port/0
        ]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

what_port() ->
    gen_server:call(?MODULE, what_port).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
-record(state, {port          :: non_neg_integer(),
                socket        :: inet:socket(),
                batch = []    :: [binary()],
                batch_max     :: non_neg_integer(),
                batch_max_age :: non_neg_integer()
               }).

init([]) ->
    {ok, Port} = application:get_env(estatsd, udp_listen_port),
    {ok, RecBuf} = application:get_env(estatsd, udp_recbuf),
    {ok, BatchMax} = application:get_env(estatsd, udp_max_batch_size),
    {ok, BatchAge} = application:get_env(estatsd, udp_max_batch_age),
    {ok, Socket} = gen_udp:open(Port, [binary, {active, once},
                                       {recbuf, RecBuf}]),
    {ok, RealPort} = inet:port(Socket),
    error_logger:info_msg("estatsd will listen on UDP ~p with recbuf ~p~n",
                          [RealPort, RecBuf]),
    error_logger:info_msg("batch size ~p with max age of ~pms~n",
                          [BatchMax, BatchAge]),
    {ok, #state{port = RealPort, socket = Socket,
                batch = [],
                batch_max = BatchMax,
                batch_max_age = BatchAge}}.

handle_call(what_port, _From, #state{port = Port} = State) ->
    {reply, {ok, Port}, State};
handle_call(_Request, _From, State) ->
    {noreply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info({udp, Socket, _Host, _Port, Bin},
            #state{batch=Batch, batch_max=Max}=State) when length(Batch) == Max ->
    error_logger:info_msg("spawn batch ~p FULL~n", [Max]),
    start_batch_worker(Batch),
    inet:setopts(Socket, [{active, once}]),
    {noreply, State#state{batch=[Bin]}};
handle_info({udp, Socket, _Host, _Port, Bin}, #state{batch=Batch,
                                                     batch_max_age=MaxAge}=State) ->
    inet:setopts(Socket, [{active, once}]),
    {noreply, State#state{batch=[Bin|Batch]}, MaxAge};
handle_info(timeout, #state{batch=Batch}=State) ->
    error_logger:info_msg("spawn batch ~p TIMEOUT~n", [length(Batch)]),
    start_batch_worker(Batch),
    {noreply, State#state{batch=[]}};
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
    proc_lib:spawn(fun() -> handle_messages(lists:reverse(Batch)) end).

handle_messages(Batch) ->
    [ handle_message(M) || M <- Batch ],
    ok.

is_legacy_message(<<"1|", _Rest/binary>>) ->
    false;
is_legacy_message(_Bin) ->
    true.

handle_message(Bin) ->
    case is_legacy_message(Bin) of
        true -> handle_legacy_message(Bin);
        false -> handle_shp_message(Bin)
    end.

handle_shp_message(Bin) ->
    [ send_metric(erlang:atom_to_binary(Type, utf8), Key, Value)
      || #shp_metric{key = Key, value = Value,
                     type = Type} <- estatsd_shp:parse_packet(Bin) ].

handle_legacy_message(Bin) ->
    try
        Lines = binary:split(Bin, <<"\n">>, [global]),
        [ parse_line(L) || L <- Lines ]
    catch
        error:Why ->
            error_logger:error_report({error, "handle_message failed",
                                       Bin, Why, erlang:get_stacktrace()})
    end.

parse_line(<<>>) ->
    skip;
parse_line(Bin) ->
    [Key, Value, Type] = binary:split(Bin, [<<":">>, <<"|">>], [global]),
    send_metric(Type, Key, Value).

send_metric(Type, Key, Value) ->
    send_estatsd_metric(Type, Key, Value).

send_estatsd_metric(Type, Key, Value)
  when Type =:= <<"ms">> orelse Type =:= <<"h">> ->
    estatsd:timing(Key, convert_value(Type, Value));
send_estatsd_metric(Type, Key, Value)
  when Type =:= <<"c">> orelse Type =:= <<"m">> ->
    estatsd:increment(Key, convert_value(Type, Value));
send_estatsd_metric(_Type, _Key, _Value) ->
    % if it isn't one of the above types, we ignore the request.
    ignored.

convert_value(<<"e">>, Value) ->
    Value;
convert_value(_Type, Value) when is_binary(Value) ->
    ?to_int(Value);
convert_value(_Type, Value) when is_integer(Value) ->
    Value.
