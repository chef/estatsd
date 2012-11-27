-module(estatsd_server_tests).

-compile([export_all]).
-include_lib("eunit/include/eunit.hrl").

setup_server() ->
    UdpMaxBatchSize = 1,
    UdpMaxBatchAge = 1000,
    UdpBuf = 524288,
    {ok, _} = capture_tcp:start_link(0),
    {ok, CapturePort} = capture_tcp:what_port(),
    application:set_env(estatsd, flush_interval, 2000),
    application:set_env(estatsd, graphite_host, "127.0.0.1"),
    application:set_env(estatsd, graphite_port, CapturePort),
    application:set_env(estatsd, udp_listen_port, 0),
    application:set_env(estatsd, udp_recbuf, UdpBuf),
    application:set_env(estatsd, udp_max_batch_size, UdpMaxBatchSize),
    application:set_env(estatsd, udp_max_batch_age, UdpMaxBatchAge),
    application:start(crypto),
    application:start(inets),
    application:start(estatsd),
    {ok, EstatsdPort} = estatsd_udp:what_port(),
    ?debugVal(EstatsdPort),
    EstatsdPort.

cleanup_server() ->
    capture_tcp:stop(),
    application:stop(estatsd).

estatsd_sanity_test_() ->
    {setup,
     fun() ->
             setup_server() end,
     fun(_) ->
             cleanup_server() end,
     fun(Port) ->
     [{"UDP metrics sent to estatsd are buffered and then sent to graphite",
       fun() ->
               {ok, S} = gen_udp:open(0),
               ok = gen_udp:send(S, "127.0.0.1", Port, <<"mycounter:10|c">>),
               ok = gen_udp:send(S, "127.0.0.1", Port, <<"mycounter:10|c">>),
               ok = gen_udp:send(S, "127.0.0.1", Port, <<"mycounter:5|d">>),
               timer:sleep(3000),
               {MsgCount, _Msgs} = capture_tcp:read(),
               ?debugVal(_Msgs),
               %% three UDP messages are sent, but these will be aggregated into
               %% a single message sent off to "graphite".
               ?assertEqual(1, MsgCount),
               %% ?assert(lists:member(<<"mycounter">>, Metrics)),
               %% MyCounter = folsom_metrics:get_metric_value(<<"mycounter">>),
               %% ?debugVal(MyCounter),
               ok
       end}]
     end}.

multi() ->
    Port = 3344,
    {ok, S} = gen_udp:open(0),
    Self = self(),
    SendUdp = fun() ->
                      Msg = <<"multicounter:10|c">>,
                      ok = gen_udp:send(S, "127.0.0.1", Port, Msg),
                      Self ! self(),
                      ok
              end,
    Pids = [ spawn(SendUdp) || _I <- lists:seq(1, 10) ],
    gather_pids(Pids),
    %% Metrics = folsom_metrics:get_metrics(),
    %% ?assertEqual(lists:usort(Metrics), Metrics),
    %% ?debugVal(Metrics),
    %% ?assert(lists:member(<<"multicounter">>, Metrics)),
    %% MultiCounter = folsom_metrics:get_metric_value(<<"multicounter">>),
    %% ?debugVal(MultiCounter),
    ok.

mass(N) ->
    Port = 3344,
    {ok, S} = gen_udp:open(0),
    Self = self(),
    SendUdp = fun() ->
                      Id = integer_to_list(crypto:rand_uniform(1, 300)),
                      Msg = iolist_to_binary([<<"metric_">>, Id, <<":10|c">>]),
                      ok = gen_udp:send(S, "127.0.0.1", Port, Msg),
                      Self ! self(),
                      ok
              end,
    Pids = [ spawn(SendUdp) || _I <- lists:seq(1, N) ],
    gather_pids(Pids),
    %% Metrics = folsom_metrics:get_metrics(),
    %% ?assertEqual(lists:usort(Metrics), lists:sort(Metrics)),
    %% ?debugVal(Metrics),
    ok.
    

gather_pids([Pid|Rest]) ->
    receive
        Pid ->
            gather_pids(Rest)
    after 2000 ->
            gather_pids(Rest)
    end;
gather_pids([]) ->
    done.

    
            
    

    
