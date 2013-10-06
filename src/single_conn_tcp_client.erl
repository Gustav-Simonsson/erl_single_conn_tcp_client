%%%----------------------------------------------------------------------------
%%% @author Gustav Simonsson <gustav.simonsson@gmail.com>
%%% @doc
%%%
%%% Dependencies: lager
%%%
%%% This TCP client is built around a few key requirements around maintaining
%%% a single, persistent connection to one out of multiple endpoints.
%%%
%%% 1. Single, persistent TCP connection to one of the endpoints.
%%% 2. One request at a time; queue multiple requests.
%%% 3. Send timeout specified on each call allowing for different send timeouts
%%%    for different messages.
%%% 4. Automatic (re) connect; connection logic is abstracted behind the
%%%    send API.
%%% 5. Endpoint failover; a list of endpoints is configured,
%%%    and if we cannot reconnect to an endpoint we try the next one.
%%% 6. Configurable timeouts for reconnection timeout and interval.
%%% 7. Queue send requests during reconnection attempts. If a request cannot be
%%%    sent over TCP due to connection being down, we keep it until its send
%%%    timeout is reached. If a reconection occurs, we sequentially go through
%%%    the queue in order.
%%%
%%% NOTE: Make sure the timeout for each request is big enough to avoid ever
%%%       getting a response after a request timeout, as such late responses
%%%       could be sent as reply to a following, possibly queued, request.
%%%       This case is intentionally not handled by this client, as it would
%%%       require packet inspection.
%%%
%%% @end
%%% Created : 04 Oct 2013 by Gustav Simonsson <gustav.simonsson@gmail.com>
%%%----------------------------------------------------------------------------
-module(single_conn_tcp_client).

-behaviour(gen_server).

%% API
-export([send/2,
         start_link/0,
         stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


-define(SERVER, ?MODULE).
-define(DEFAULT_TCP_OPTS, [binary,
                           {active, true},
                           {packet, 0},
                           {reuseaddr, true}
                       ]).
-record(s, {sock,
            waiting,
            q,
            endpoints,
            reconn_interval,
            conn_timeout
           }).
%%%============================================================================
%%% API
%%%============================================================================
-spec send(Msg :: binary(), Timeout :: pos_integer()) ->
                  timeout | {ok, Response :: binary()}.
send(Msg, Timeout) ->
    %% Set gen_server:call/3 timeout to infinity as internal timeouts are used.
    gen_server:call(?SERVER, {send, Msg, Timeout}, infinity).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec stop() -> ok.
stop() ->
    gen_server:cast(?SERVER, stop).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================
init([]) ->
    %% Never trust the user. Validate config.
    try
        %% TODO: change endpoints configure to your app using the client.
        Endpoints = application:get_env(gs_util, tcp_endpoints),
        true = (is_list(Endpoints) andalso
                length(Endpoints) > 0),
        Available =
            fun({Host, Port}) when is_integer(Port), Port >= 0, Port =< 65535 ->
                    case inet:gethostbyname(Host) of
                        {ok, Hostent} ->
                            lager:info("~p: hostent for configured host: ~p,"
                                  "Hostent: ~p~n", [?MODULE, Host, Hostent]),
                            true;
                        {error, Posix} ->
                            %% As long as one host is available (check below)
                            %% this can fail during upstart.
                            lager:warning("~p: hostent error for configured host: ~p,"
                                     "Error: ~p~n", [?MODULE, Host, Posix]),
                            false
                    end
            end,
        true = lists:any(Available, Endpoints),
        RI = el_app:get_env(gs_util, tcp_reconn_interval),
        true = (is_integer(RI) and (RI > 500)),
        CT = el_app:get_env(gs_util, tcp_conn_timeout),
        true = (is_integer(CT) and (CT > 400)),
        send_reconnect(),
        {ok, #s{reconn_interval = RI,
                conn_timeout = CT,
                endpoints = Endpoints,
                sock = dead,
                q = [],
                waiting = false}}
    catch E:R ->
            Stack = erlang:get_stacktrace(),
            lager:error("~p: Invalid config. Please check config. "
                   "Error: ~p, Reason: ~p, Stack: ~p~n",
                   [?MODULE,E,R,Stack]),
            throw({error, invalid_config_check_logs, E, R, Stack})
    end.

handle_call({send, Msg, Timeout}, From, #s{sock = dead, q = Reqs} = S) ->
    NewReqs = queue_req(Msg, Timeout, From, Reqs),
    {noreply, S#s{q = NewReqs}};
handle_call({send, Msg, Timeout}, From, #s{waiting = true, q = Reqs} = S) ->
    NewReqs = queue_req(Msg, Timeout, From, Reqs),
    {noreply, S#s{q = NewReqs}};
handle_call({send, Msg, Timeout}, From, #s{sock = Sock, q = []} = S) ->
    erlang:send_after(Timeout, ?SERVER, {req_timeout, ReqRef = make_ref()}),
    send_tcp(Sock, Msg),
    {noreply, S#s{waiting = true, q = [{ReqRef, From, Msg}]}};
handle_call(Call, From, S) ->
    lager:error("~p: Unmatched call ~p from ~p~n", [?MODULE, Call, From]),
    {noreply, S}.

handle_cast(stop, S) ->
    lager:info("~p: Stopping server", [?MODULE]),
    {stop, normal, S};
handle_cast(Cast, S) ->
    lager:error("~p: Unmatched cast ~p~n", [?MODULE, Cast]),
    {noreply, S}.

handle_info({tcp_closed, _}, S) ->
    lager:info("~p: TCP socket closed. Attempt immediate reconnect...~n", [?MODULE]),
    send_reconnect(),
    {noreply, S#s{sock = dead}};
handle_info({tcp_error, _, Reason}, S) ->
    lager:error("~p: TCP socket error: ~p. Attempt immediate reconnect...~n",
           [?MODULE, Reason]),
    send_reconnect(),
    {noreply, S#s{sock = dead}};
handle_info({tcp, _, Msg}, #s{sock = dead} = S) ->
    lager:info("~p: Received TCP msg: ~p when socket is dead~n",
          [?MODULE, to_bin_format(Msg)]),
    {noreply, S};
handle_info({tcp, _, Response}, #s{sock = Sock, q = Reqs} = S) ->
    case handle_reply(Response, Reqs) of
        [] ->
            {noreply, S#s{waiting = false, q = []}};
        [{_ReqRef, _From, Msg} | _] = NewReqs ->
            send_tcp(Sock, Msg),
            {noreply, S#s{waiting = true, q = NewReqs}}
    end;
handle_info(reconnect, S) ->
    #s{sock = dead, endpoints = Es, q = Reqs,
       reconn_interval = RI, conn_timeout = CT} = S,
    case reconnect(Es, CT) of
        dead ->
            erlang:send_after(RI, ?SERVER, reconnect),
            {noreply, S#s{sock = dead}};
        Sock ->
            case Reqs of
                [] -> {noreply, S#s{sock = Sock, endpoints = Es}};
                [{_ReqRef, _From, Msg} | _ ] = NewReqs ->
                    send_tcp(Sock, Msg),
                    {noreply, S#s{sock = Sock, endpoints = Es,
                                  waiting = true, q = NewReqs}}
            end
    end;
handle_info({req_timeout, ReqRef}, #s{waiting = Waiting,
                                      q = Reqs} = S) ->
    lager:info("~p: request timeout.~n", [?MODULE]),
    {NewReqs, ForThisReq} = req_timeout(ReqRef, Reqs),
    {noreply, S#s{waiting = Waiting and not ForThisReq, q = NewReqs}};
handle_info(Info, S) ->
    lager:error("~p: Unmatched info ~p~n", [?MODULE, Info]),
    {noreply, S}.

terminate(Reason, #s{sock = Sock}) when is_port(Sock) ->
    lager:info("~p: terminating server: ~p~n", [?MODULE, Reason]),
    gen_tcp:close(Sock),
    ok;
terminate(Reason, _S) ->
    lager:info("~p: terminating server: ~p~n", [?MODULE, Reason]),
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%%============================================================================
%%% Internal functions
%%%============================================================================
queue_req(Msg, Timeout, From, Reqs) ->
    erlang:send_after(Timeout, ?SERVER, {req_timeout, ReqRef = make_ref()}),
    NewReqs = lists:append(Reqs, [{ReqRef, From, Msg}]),
    lager:warning("~p: Queuing req. Queue len: ~p~n",
             [?MODULE, length(NewReqs)]),
    NewReqs.

req_timeout(ReqRef, [{ReqRef, From, _Msg} | NewReqs]) ->
    gen_server:reply(From, timeout),
    {NewReqs, true};
req_timeout(ReqRef, Reqs) ->
    case lists:keytake(ReqRef, 1, Reqs) of
        false -> %% Timeout triggered after we've already replied
            {Reqs, false};
        {value, {ReqRef, From, _Msg}, NewReqs} ->
            gen_server:reply(From, timeout),
            {NewReqs, false}
    end.

%% Assume send timeout is big enough to never get a response after the request
%% has timed out. A response is always assumed to be for the current request,
%% since we do not inspect the response.
handle_reply(Response, [{_ReqRef, From, _Msg} | ReqsLeft]) ->
    lager:info("~p: Received TCP msg: ~p~n",
          [?MODULE, to_bin_format(Response)]),
    gen_server:reply(From, {ok, Response}),
    ReqsLeft;
handle_reply(Response, []) ->
    lager:info("~p: Received TCP msg after request timeout: ~p~n",
          [?MODULE, to_bin_format(Response)]),
    [].

send_tcp(Sock, Msg) ->
    lager:info("~p: Sending  TCP msg: ~p~n", [?MODULE, to_bin_format(Msg)]),
    gen_tcp:send(Sock, Msg).

send_reconnect() ->
    ?SERVER ! reconnect.

reconnect([{Host, Port} | Endpoints], CT) ->
    lager:info("~p: Attempt connection to: ~p~n", [?MODULE, {Host, Port}]),
    case gen_tcp:connect(Host, Port,
                         ?DEFAULT_TCP_OPTS, CT) of
        {ok, Sock} ->
            lager:info("~p: Connected to endpoint: ~p~n", [?MODULE, {Host, Port}]),
            Sock;
        {error, Posix} ->
            lager:error("~p: TCP connection to ~p failed: ~p~n",
                   [?MODULE, {Host, Port}, Posix]),
            case Endpoints of
                [] ->
                    dead;
                _ ->
                    reconnect(Endpoints, CT)
            end
    end.
to_bin_format(Bin) ->
    Bin.

%%%============================================================================
%%% Eunit
%%%============================================================================

-include_lib("eunit/include/eunit.hrl").

-define(TEST_TCP_PORT, 7600).
-define(TEST_TCP_OPTS, [binary,
                        {active, false},
                        {packet, 0},
                        {reuseaddr, true}]).

%%%============================================================================
%%% Test generator / fixtures
%%%============================================================================
single_conn_tcp_client_test_() ->
    Tests =
        [
         {"startup and send",
          fun test_startup_and_send/0},
         {"send with queue",
          fun test_send_with_queue/0},
         {"response after request timeout",
          fun test_response_after_request_timeout/0},
         {"reconn with one endpoint",
          fun test_reconn_one_endpoint/0},
         {"reconn with one endpoint with queue",
          fun test_reconn_one_endpoint_with_queue/0},
         {"reconn multiple endpoints",
          fun test_reconn_multiple_endpoints/0},
         {"multiple reconn intervals",
          fun test_multiple_reconn_intervals_with_queue/0},
         {"reconn with one endpoint due to tcp error",
          fun test_reconn_multiple_endpoint_tcp_error/0}
        ],
    {setup,
     fun setup/0,
     fun teardown/1,
     fun (_) -> {timeout, 20, Tests} end
    }.

setup() ->
    application:set_env(gs_util, tcp_endpoints,
                       [{"localhost", ?TEST_TCP_PORT}]),
    application:set_env(gs_util, tcp_reconn_interval, 1000),
    application:set_env(gs_util, tcp_conn_timeout, 500),
    lager:start(),
    ok.

teardown(_) ->
    ok.

%%%============================================================================
%%% Tests
%%%============================================================================
test_startup_and_send() ->
    setup_server_and_client(),
    Response1 = single_conn_tcp_client:send(<<"ignored_by_test_server">>, 100),
    ?assertEqual(timeout, Response1),
    Response2 = single_conn_tcp_client:send(<<2>>, 100),
    ?assertEqual({ok, <<3>>}, Response2),
    teardown_server_and_client(),
    ok.

test_send_with_queue() ->
    setup_server_and_client(),
    TestProc = self(),
    spawn(fun() -> TestProc ! {a, single_conn_tcp_client:send(<<1>>, 100)} end),
    spawn(fun() -> TestProc ! {b, single_conn_tcp_client:send(<<3>>, 100)} end),
    spawn(fun() -> TestProc ! {c, single_conn_tcp_client:send(<<5>>, 100)} end),
    receive {a,Ra} -> ?assertEqual({ok, <<2>>}, Ra) end,
    receive {b,Rb} -> ?assertEqual({ok, <<4>>}, Rb) end,
    receive {c,Rc} -> ?assertEqual({ok, <<6>>}, Rc) end,
    teardown_server_and_client(),
    ok.

test_response_after_request_timeout() ->
    setup_server_and_client(),
    Response1 = single_conn_tcp_client:send(<<"ignored_by_test_server">>, 0),
    ?assertEqual(timeout, Response1),
    single_conn_tcp_client ! {tcp, fake_socket, <<"fake_response">>},
    timer:sleep(100),
    teardown_server_and_client(),
    ok.

test_reconn_one_endpoint() ->
    setup_server_and_client(),
    TestProc = self(),
    single_conn_tcp_client:send(<<"stop">>, 100),

    spawn(fun() -> TestProc ! {a, single_conn_tcp_client:send(<<1>>, 100)} end),
    receive {a,Ra} -> ?assertEqual(timeout, Ra) end,

    spawn(fun() -> server(?TEST_TCP_PORT) end),
    timer:sleep(1200),

    spawn(fun() -> TestProc ! {b, single_conn_tcp_client:send(<<3>>, 100)} end),
    receive {b,Rb} -> ?assertEqual({ok, <<4>>}, Rb) end,

    teardown_server_and_client(),
    ok.

test_reconn_one_endpoint_with_queue() ->
    setup_server_and_client(),
    TestProc = self(),
    single_conn_tcp_client:send(<<"stop">>, 100),

    spawn(fun() -> TestProc ! {a, single_conn_tcp_client:send(<<1>>, 2000)} end),
    spawn(fun() -> TestProc ! {b, single_conn_tcp_client:send(<<3>>, 2000)} end),
    spawn(fun() -> TestProc ! {c, single_conn_tcp_client:send(<<5>>, 2000)} end),
    spawn(fun() ->
                  timer:sleep(100),
                  server(?TEST_TCP_PORT)
          end),
    receive {a,Ra} -> ?assertEqual({ok, <<2>>}, Ra) end,
    receive {b,Rb} -> ?assertEqual({ok, <<4>>}, Rb) end,
    receive {c,Rc} -> ?assertEqual({ok, <<6>>}, Rc) end,
    teardown_server_and_client(),
    ok.

test_reconn_multiple_endpoints() ->
    application:set_env(gs_util, tcp_endpoints,
                       [{"localhost", ?TEST_TCP_PORT},
                        {"localhost", ?TEST_TCP_PORT + 1},
                        {"localhost", ?TEST_TCP_PORT + 2}
                       ]),
    setup_server_and_client(),
    TestProc = self(),
    %% 1. Take down the first, only active, endpoint.
    %% 2. Send a request which times out.
    %% 3. Bring up the first endpoint again.
    %% 4. Send request which gets reply
    single_conn_tcp_client:send(<<"stop">>, 100),
    spawn(fun() -> TestProc ! {a, single_conn_tcp_client:send(<<1>>, 100)} end),
    receive {a,Ra} -> ?assertEqual(timeout, Ra) end,
    spawn(fun() -> server(?TEST_TCP_PORT) end),
    timer:sleep(1200),
    spawn(fun() -> TestProc ! {b, single_conn_tcp_client:send(<<3>>, 100)} end),
    receive {b,Rb} -> ?assertEqual({ok, <<4>>}, Rb) end,

    %% 1. Take down first endpoint
    %% 2. Send a message which times out
    %% 3. Bring third endpoint up
    %% 4. Send a request which gets reply
    single_conn_tcp_client:send(<<"stop">>, 100),
    spawn(fun() -> TestProc ! {c, single_conn_tcp_client:send(<<5>>, 100)} end),
    receive {c,Rc} -> ?assertEqual(timeout, Rc) end,
    spawn(fun() -> server(?TEST_TCP_PORT + 2) end),
    timer:sleep(1200),
    spawn(fun() -> TestProc ! {d, single_conn_tcp_client:send(<<7>>, 100)} end),
    receive {d,Rd} -> ?assertEqual({ok, <<8>>}, Rd) end,

    teardown_server_and_client(),
    ok.

test_multiple_reconn_intervals_with_queue() ->
    application:set_env(gs_util, tcp_endpoints,
                       [{"localhost", ?TEST_TCP_PORT},
                        {"localhost", ?TEST_TCP_PORT + 1},
                        {"localhost", ?TEST_TCP_PORT + 2}
                       ]),
    setup_server_and_client(),
    TestProc = self(),
    %% 1. Take down the first, only active, endpoint.
    %% 2. Send some request of which some will timeout before reconnection.
    %% 3. Bring up the second endpoint.
    %% 4. Requestes not yet timed out gets response
    single_conn_tcp_client:send(<<"stop">>, 100),
    spawn(fun() -> TestProc ! {a, single_conn_tcp_client:send(<<1>>, 100)} end),
    receive {a,Ra} -> ?assertEqual(timeout, Ra) end,
    spawn(fun() ->
                  timer:sleep(3000),
                  server(?TEST_TCP_PORT + 1)
          end),

    spawn(fun() -> TestProc ! {a, single_conn_tcp_client:send(<<3>>,  2000)} end),
    timer:sleep(100),
    spawn(fun() -> TestProc ! {b, single_conn_tcp_client:send(<<5>>,  1000)} end),
    spawn(fun() -> TestProc ! {c, single_conn_tcp_client:send(<<7>>,  4000)} end),
    spawn(fun() -> TestProc ! {d, single_conn_tcp_client:send(<<9>>,  4500)} end),
    spawn(fun() -> TestProc ! {e, single_conn_tcp_client:send(<<11>>, 5000)} end),

    receive {a,Rb} -> ?assertEqual(timeout, Rb) end,
    receive {b,Rc} -> ?assertEqual(timeout, Rc) end,
    receive {c,Rd} -> ?assertEqual({ok, <<8>>}, Rd) end,
    receive {d,Re} -> ?assertEqual({ok, <<10>>}, Re) end,
    receive {e,Rf} -> ?assertEqual({ok, <<12>>}, Rf) end,

    teardown_server_and_client(),
    ok.

test_reconn_multiple_endpoint_tcp_error() ->
    application:set_env(gs_util, tcp_endpoints,
                       [{"localhost", ?TEST_TCP_PORT},
                        {"localhost", ?TEST_TCP_PORT + 1},
                        {"localhost", ?TEST_TCP_PORT + 2}
                       ]),
    spawn(fun() -> server(?TEST_TCP_PORT + 1) end),
    single_conn_tcp_client:start_link(),
    timer:sleep(100),
    spawn(fun() -> server(?TEST_TCP_PORT) end),
    timer:sleep(100),
    single_conn_tcp_client ! {tcp_error, fake_socket, fake_reason},

    TestProc = self(),
    spawn(fun() -> TestProc ! {a, single_conn_tcp_client:send(<<1>>, 100)} end),
    receive {a,Ra} -> ?assertEqual({ok, <<2>>}, Ra) end,

    teardown_server_and_client(),
    ok.

%%%============================================================================
%%% Test helpers
%%%============================================================================
setup_server_and_client() ->
    spawn(fun() -> server(?TEST_TCP_PORT) end),
    single_conn_tcp_client:start_link(),
    ok.

server(Port) ->
    {ok, LS} = gen_tcp:listen(Port, ?TEST_TCP_OPTS),
    {ok, Sock} = gen_tcp:accept(LS),
    recv_loop(LS, Sock, accept).

recv_loop(LS, Sock, Mode) ->
    case gen_tcp:recv(Sock, 0) of
        {error, _} ->
            stop;
        {ok, Packet} ->
            case Packet of
                <<ByteInt>> = Msg ->
                    lager:info("Test server replying to msg: ~p~n", [Msg]),
                    ok = gen_tcp:send(Sock, <<(ByteInt + 1)>>),
                    recv_loop(LS, Sock, Mode);
                <<"stop">> ->
                    lager:info("Test server stopping~n", []),
                    ok = gen_tcp:send(Sock, <<"stopped">>),
                    gen_tcp:close(Sock),
                    gen_tcp:close(LS);
                Other ->
                    lager:info("Test server received unmatched msg ~p~n", [Other]),
                    recv_loop(LS, Sock, Mode)
            end
    end.

teardown_server_and_client() ->
    single_conn_tcp_client:stop(),
    ok.
