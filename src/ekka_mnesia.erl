%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(ekka_mnesia).

-include("ekka.hrl").

%% Start and stop mnesia
-export([start/0, ensure_started/0, ensure_stopped/0, connect/1]).

%% Cluster mnesia
-export([join_cluster/1, leave_cluster/0, remove_from_cluster/1,
         cluster_status/0, cluster_status/1, cluster_view/0,
         cluster_nodes/1, running_nodes/0]).

-export([is_node_in_cluster/0, is_node_in_cluster/1]).

%% Dir, schema and tables
-export([data_dir/0, copy_schema/1, delete_schema/0, del_schema_copy/1,
         create_table/2, copy_table/1, copy_table/2]).

%%--------------------------------------------------------------------
%% Start and init mnesia
%%--------------------------------------------------------------------

%% @doc Start mnesia database
%% 启动 mnesia 数据库
-spec(start() -> ok | {error, term()}).
start() ->
    ensure_ok(ensure_data_dir()),
    ensure_ok(init_schema()),
    ok = mnesia:start(),
    init_tables(),
    wait_for(tables).

%% @private
ensure_data_dir() ->
    case filelib:ensure_dir(data_dir()) of
        ok              -> ok;
        {error, Reason} -> {error, Reason}
    end.

%% @doc Data dir
-spec(data_dir() -> string()).
data_dir() -> mnesia:system_info(directory).

%% @doc Ensure mnesia started
%% 确保 mnesia启动 
-spec(ensure_started() -> ok | {error, any()}).
ensure_started() ->
    ok = mnesia:start(), wait_for(start).

%% @doc Ensure mnesia stopped
%% 确保 mnesia 停止
-spec(ensure_stopped() -> ok | {error, any()}).
ensure_stopped() ->
    stopped = mnesia:stop(), wait_for(stop).

%% @private
%% @doc Init mnesia schema or tables.
init_schema() ->
    case mnesia:system_info(extra_db_nodes) of
        []    -> mnesia:create_schema([node()]);
        [_|_] -> ok
    end.

%% @private
%% @doc Init mnesia tables.
init_tables() ->
    case mnesia:system_info(extra_db_nodes) of
        []    -> create_tables();
        [_|_] -> copy_tables()
    end.

%% @doc Create mnesia tables.
create_tables() ->
    ekka_boot:apply_module_attributes(boot_mnesia).

%% @doc Copy mnesia tables.
copy_tables() ->
    ekka_boot:apply_module_attributes(copy_mnesia).

%% @doc Create mnesia table.
-spec(create_table(Name:: atom(), TabDef :: list()) -> ok | {error, any()}).
create_table(Name, TabDef) ->
    ensure_tab(mnesia:create_table(Name, TabDef)).

%% @doc Copy mnesia table.
-spec(copy_table(Name :: atom()) -> ok).
copy_table(Name) ->
    copy_table(Name, ram_copies).

-spec(copy_table(Name:: atom(), ram_copies | disc_copies) -> ok).
copy_table(Name, RamOrDisc) ->
    ensure_tab(mnesia:add_table_copy(Name, node(), RamOrDisc)).

%% @doc Copy schema.
copy_schema(Node) ->
    case mnesia:change_table_copy_type(schema, Node, disc_copies) of
        {atomic, ok} ->
            ok;
        {aborted, {already_exists, schema, Node, disc_copies}} ->
            ok;
        {aborted, Error} ->
            {error, Error}
    end.

%% @doc Force to delete schema.
delete_schema() ->
    mnesia:delete_schema([node()]).

%% @doc Delete schema copy
del_schema_copy(Node) ->
    case mnesia:del_table_copy(schema, Node) of
        {atomic, ok} -> ok;
        {aborted, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Cluster mnesia
%%--------------------------------------------------------------------

%% @doc Join the mnesia cluster
%% 加入mnesia 集群
-spec(join_cluster(node()) -> ok).
join_cluster(Node) when Node =/= node() ->
    %% Stop mnesia and delete schema first
    ensure_ok(ensure_stopped()),
    ensure_ok(delete_schema()),
    %% Start mnesia and cluster to node
    ensure_ok(ensure_started()),
    ensure_ok(connect(Node)),
    ensure_ok(copy_schema(node())),
    %% Copy tables
    copy_tables(),
    ensure_ok(wait_for(tables)).

%% @doc Cluster status
%% 集群状态
-spec(cluster_status() -> list()).
cluster_status() ->
    Running = mnesia:system_info(running_db_nodes),
    Stopped = mnesia:system_info(db_nodes) -- Running,
    case Stopped =:= [] of
        true  -> [{running_nodes, Running}];
        false -> [{running_nodes, Running},
                  {stopped_nodes, Stopped}]
    end.

%% @doc Cluster status of the node
%% 节点在集群的状态
-spec(cluster_status(node()) -> running | stopped | false).
cluster_status(Node) ->
    case is_node_in_cluster(Node) of
        true  ->
            case lists:member(Node, running_nodes()) of
                true  -> running;
                false -> stopped
            end;
        false ->
            false
    end.

-spec(cluster_view() -> {[node()], [node()]}).
cluster_view() ->
    list_to_tuple([lists:sort(cluster_nodes(Status))
                   || Status <- [running, stopped]]).

%% @doc This node try leave the cluster
%% 这个节点试着离开 集群
-spec(leave_cluster() -> ok | {error, any()}).
leave_cluster() ->
    case running_nodes() -- [node()] of
        [] ->
            {error, node_not_in_cluster};
        Nodes ->
            case lists:any(fun(Node) ->
                            case leave_cluster(Node) of
                                ok               -> true;
                                {error, _Reason} -> false
                            end
                          end, Nodes) of
                true  -> ok;
                false -> {error, {failed_to_leave, Nodes}}
            end
    end.

-spec(leave_cluster(node()) -> ok | {error, any()}).
leave_cluster(Node) when Node =/= node() ->
    case is_running_db_node(Node) of
        true ->
            ensure_ok(ensure_stopped()),
            ensure_ok(rpc:call(Node, ?MODULE, del_schema_copy, [node()])),
            ensure_ok(delete_schema());
            %%ensure_ok(start()); %% restart?
        false ->
            {error, {node_not_running, Node}}
    end.

%% @doc Remove node from mnesia cluster.
%% 从mnesia 中移除Node 节点
-spec(remove_from_cluster(node()) -> ok | {error, any()}).
remove_from_cluster(Node) when Node =/= node() ->
    %% 是否在集群里
    %% 是否是运行db的节点
    case {is_node_in_cluster(Node), is_running_db_node(Node)} of
        {true, true} ->
            ensure_ok(rpc:call(Node, ?MODULE, ensure_stopped, [])),
            mnesia_lib:del(extra_db_nodes, Node),
            ensure_ok(del_schema_copy(Node)),
            ensure_ok(rpc:call(Node, ?MODULE, delete_schema, []));
        {true, false} ->
            mnesia_lib:del(extra_db_nodes, Node),
            ensure_ok(del_schema_copy(Node));
            %ensure_ok(rpc:call(Node, ?MODULE, delete_schema, []));
        {false, _} ->
            {error, node_not_in_cluster}
    end.

%% @doc Is this node in mnesia cluster?
%% 这个节点是否在 mnesia 集群里
is_node_in_cluster() ->
    ekka_mnesia:cluster_nodes(all) =/= [node()].

%% @doc Is the node in mnesia cluster?
%% Node 节点是否在 mnesia 集群里
-spec(is_node_in_cluster(node()) -> boolean()).
is_node_in_cluster(Node) when Node =:= node() ->
    is_node_in_cluster();
is_node_in_cluster(Node) ->
    lists:member(Node, cluster_nodes(all)).

%% @private
%% @doc Is running db node.
%% 是否是运行db的节点
is_running_db_node(Node) ->
    lists:member(Node, running_nodes()).

%% @doc Cluster with node.
-spec(connect(node()) -> ok | {error, any()}).
connect(Node) ->
    case mnesia:change_config(extra_db_nodes, [Node]) of
        {ok, [Node]} -> ok;
        {ok, []}     -> {error, {failed_to_connect_node, Node}};
        Error        -> Error
    end.

%% @doc Running nodes.
%% 运行的节点列表
-spec(running_nodes() -> list(node())).
running_nodes() -> cluster_nodes(running).

%% @doc Cluster nodes.
%% 集群节点列表 all 所有的  running 正在运行的 stopped 已经停止的
-spec(cluster_nodes(all | running | stopped) -> [node()]).
cluster_nodes(all) ->
    mnesia:system_info(db_nodes);
cluster_nodes(running) ->
    mnesia:system_info(running_db_nodes);
cluster_nodes(stopped) ->
    cluster_nodes(all) -- cluster_nodes(running).

%% @private
ensure_ok(ok) -> ok;
ensure_ok({error, {_Node, {already_exists, _Node}}}) -> ok;
ensure_ok({badrpc, Reason}) -> throw({error, {badrpc, Reason}});
ensure_ok({error, Reason}) -> throw({error, Reason}).

%% @private
ensure_tab({atomic, ok})                             -> ok;
ensure_tab({aborted, {already_exists, _Name}})       -> ok;
ensure_tab({aborted, {already_exists, _Name, _Node}})-> ok;
ensure_tab({aborted, Error})                         -> Error.

%% @doc Wait for mnesia to start, stop or tables ready.
%% 等待 mnesia 启动， 停止 或者准备好表
-spec(wait_for(start | stop | tables) -> ok | {error, Reason :: atom()}).
wait_for(start) ->
    case mnesia:system_info(is_running) of
        yes      -> ok;
        no       -> {error, mnesia_unexpectedly_stopped};
        stopping -> {error, mnesia_unexpectedly_stopping};
        starting -> timer:sleep(1000), wait_for(start)
    end;

wait_for(stop) ->
    case mnesia:system_info(is_running) of
        no       -> ok;
        yes      -> {error, mnesia_unexpectedly_running};
        starting -> {error, mnesia_unexpectedly_starting};
        stopping -> timer:sleep(1000), wait_for(stop)
    end;

wait_for(tables) ->
    Tables = mnesia:system_info(local_tables),
    case mnesia:wait_for_tables(Tables, 150000) of
        ok                   -> ok;
        {error, Reason}      -> {error, Reason};
        {timeout, BadTables} -> {error, {timeout, BadTables}}
    end.

