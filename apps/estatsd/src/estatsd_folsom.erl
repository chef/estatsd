-module(estatsd_folsom).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-define(ALL_METRICS, all_metrics_keys).
-include_lib("folsom/include/folsom.hrl").

%-include_lib("eunit/include/eunit.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0, ensure_metric/2]).

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

ensure_metric(Key, Type) ->
    {Table, Fun} = table_for_type(Type),
    gen_server:call(?SERVER, {ensure_metric, Key, Type, Table, Fun}, infinity).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init([]) ->
    ets:new(?ALL_METRICS, [set, named_table, public]),
    {ok, undefined}.

handle_call({ensure_metric, Key, Type, Table, Fun}, _From, State) ->
    Status = case ets:member(?ALL_METRICS, {Key, Table}) of
                 true ->
                     already_exists;
                 false ->
                     folsom_metrics:Fun(Key),
                     ets:insert(?ALL_METRICS, {{Key, Table}, true}),
                     %%error_logger:info_msg("created|~p|~p|~p~n",
                     %%                      [{Key, Table}, Fun, Type]),
                    created
            end,
    {reply, {ok, Status}, State};
handle_call(_Request, _From, State) ->
    {noreply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

table_for_type(Type) when Type =:= <<"d">> orelse Type =:= <<"c">> ->
    {?COUNTER_TABLE, new_counter};
table_for_type(Type) when Type =:= <<"ms">> orelse Type =:= <<"h">> ->
    {?HISTOGRAM_TABLE, new_histogram};
table_for_type(<<"e">>) ->
    {?HISTORY_TABLE, new_history};
table_for_type(<<"g">>) ->
    {?GAUGE_TABLE, new_gauge};
table_for_type(<<"m">>) ->
    {?METER_TABLE, new_meter};
table_for_type(BadType) ->
    erlang:error({error, {bad_type, BadType}}).
