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

%% 集群策略  通用行为
-module(ekka_cluster_strategy).

-ifdef(use_specs).

-type(options() :: list(proplists:property())).

%% 发现
-callback(discover(options()) -> {ok, list(node())} | {error, term()}).

%% 锁
-callback(lock(options()) -> ok | ignore | {error, term()}).

%% 解锁
-callback(unlock(options()) -> ok | ignore | {error, term()}).

%% 注册
-callback(register(options()) -> ok | ignore | {error, term()}).

%% 移除注册
-callback(unregister(options()) -> ok | ignore | {error, term()}).

-else.

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{discover, 1}, {lock, 1}, {unlock, 1}, {register, 1}, {unregister, 1}];
behaviour_info(_Other) ->
    undefined.

-endif.

