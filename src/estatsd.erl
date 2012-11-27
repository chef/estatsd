-module(estatsd).

-export([
         increment/1, increment/2, increment/3,
         decrement/1, decrement/2, decrement/3,
         timing/2,
         start/0, stop/0
        ]).

-define(SERVER, estatsd_server).

%% @spec start() -> ok
%% @doc Start the estatsd server.
start() ->
    application:start(estatsd).

%% @spec stop() -> ok
%% @doc Stop the estatsd server.
stop() ->
    application:stop(estatsd).

% Convenience: just give it the now() tuple when the work started
timing(Key, StartTime = {_,_,_}) ->
    Dur = erlang:round(timer:now_diff(erlang:now(), StartTime)/1000),
    timing(Key,Dur);

% Log timing information, ms
timing(Key, Duration) when is_integer(Duration) -> 
    gen_server:cast(?SERVER, {timing, Key, Duration});

timing(Key, Duration) -> 
    gen_server:cast(?SERVER, {timing, Key, erlang:round(Duration)}).


% Increments one or more stats counters
increment(Key) -> increment(Key, 1, 1).
increment(Key, Amount) -> increment(Key, Amount, 1).
increment(Key, Amount, Sample) ->
    gen_server:cast(?SERVER, {increment, Key, Amount, Sample}).

decrement(Key) -> decrement(Key, -1, 1).
decrement(Key, Amount) -> decrement(Key, Amount, 1).
decrement(Key, Amount, Sample) ->
    increment(Key, 0 - Amount, Sample).


