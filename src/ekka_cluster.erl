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

%% @doc Ekka Cluster with Mnesia Database.
-module(ekka_cluster).

%% Cluster API
-export([join/1, leave/0, force_leave/1, status/0]).

%% RPC Call for Cluster Management
-export([prepare/1, heal/1, reboot/0]).

%% @doc Join the cluster
%% 加入集群
-spec(join(node()) -> ok | ignore | {error, any()}).
%% 如果是节点是当前节点，忽略
join(Node) when Node =:= node() ->
    ignore;
join(Node) when is_atom(Node) ->
    %% 节点是否在集群里面
    %% 节点是否运行 ekka程序
    case {ekka_mnesia:is_node_in_cluster(Node), ekka_node:is_running(Node, ekka)} of
        {false, true} ->
            prepare(join), ok = ekka_mnesia:join_cluster(Node), reboot();
        {false, false} ->
            {error, {node_down, Node}};
        {true, _} ->
            {error, {already_in_cluster, Node}}
    end.

%% @doc Leave from the cluster.
%% 离开这个集群
-spec(leave() -> ok | {error, any()}).
leave() ->
    case ekka_mnesia:running_nodes() -- [node()] of
        [_|_] ->
            prepare(leave), ok = ekka_mnesia:leave_cluster(), reboot();
        [] ->
            {error, node_not_in_cluster}
    end.

%% @doc Force a node leave from cluster.
%% 强制一个节点离开这个集群
-spec(force_leave(node()) -> ok | ignore | {error, term()}).
%% 如果该节点是当前节点  忽略
force_leave(Node) when Node =:= node() ->
    ignore;
force_leave(Node) ->
    %% 节点是否在集群里面  然后 rpc 调用Node节点执行当前模块的 prepare 函数，参数是leave
    case ekka_mnesia:is_node_in_cluster(Node)
         andalso rpc:call(Node, ?MODULE, prepare, [leave]) of
        ok ->
            %% 把节点移除集群
            case ekka_mnesia:remove_from_cluster(Node) of
                ok    -> rpc:call(Node, ?MODULE, reboot, []);
                Error -> Error
            end;
        false ->
            {error, node_not_in_cluster};
        {badrpc, nodedown} ->
            ekka_membership:announce({force_leave, Node}),
            ekka_mnesia:remove_from_cluster(Node);
        {badrpc, Reason} ->
            {error, Reason}
    end.

%% @doc Heal partitions
%% 恢复分区
-spec(heal(shutdown | reboot) -> ok | {error, term()}).
heal(shutdown) ->
    prepare(heal), ekka_mnesia:ensure_stopped();
heal(reboot) ->
    ekka_mnesia:ensure_started(), reboot().

%% @doc Prepare to join or leave the cluster.
%% 准备加入或离开这个集群
-spec(prepare(join | leave) -> ok | {error, term()}).
prepare(Action) ->
    ekka_membership:announce(Action),
    case ekka:callback(prepare) of
        {ok, Prepare} -> Prepare(Action);
        undefined     -> application:stop(ekka)
    end.

%% @doc Reboot after join or leave cluster.
%% 离开或加入这个 集群后重启
-spec(reboot() -> ok | {error, term()}).
reboot() ->
    case ekka:callback(reboot) of
        {ok, Reboot} -> Reboot();
        undefined    -> ekka:start()
    end.

%% @doc Cluster status.
%% 集群状态
status() -> ekka_mnesia:cluster_status().

