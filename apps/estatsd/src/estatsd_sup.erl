-module(estatsd_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    Children = [
                {estatsd_server, 
                 {estatsd_server, start_link, []},
                 permanent, 5000, worker, [estatsd_server]},

                {estatsd_udp,
                 {estatsd_udp, start_link, []},
                  permanent, 5000, worker, [estatsd_udp]}
               ],
    {ok, { {one_for_one, 10000, 10}, Children} }.
