-module(woody).

-export([new/0,
         is_woody/1,
         query/2,
         encode/1,
         update/2]).

-record(woody_tree,         {content}).
-record(woody_result,       {content}).
-record(woody_set,          {content}).
-record(woody_zset,         {content}).
-record(woody_trie,         {content}).
-record(woody_zset_result,  {content, scores}).
-record(woody_trie_result,  {content}).

encode(#woody_result{content=Content}) ->
    encode_value(Content).

new() ->
    #woody_tree{}.

is_woody(#woody_tree{}) ->
    true;

is_woody(_) ->
    false.

query(Query, #woody_tree{content=Content}) ->
    case process_query(Query, Content) of
        {error, _}=Error ->
            Error;
        R ->
            {ok, #woody_result{content=R}}
    end.

update(Update, #woody_tree{content=Content}) ->
    case process_update(Update, Content) of
        {error, _}=Error ->
            Error;
        C ->
            {ok, #woody_tree{content=C}}
    end.

encode_value({dict, Dict}) ->
    Fun = fun(K, V, Acc) ->
                  maps:put(K, encode_value(V), Acc)
          end,
    dict:fold(Fun, #{}, Dict);
encode_value(#woody_trie_result{content=TrieResult}) ->
    Fun = fun(_, V) -> encode_value(V) end,
    maps:map(Fun, TrieResult);
encode_value(#woody_set{content=Set}) ->
    sets:to_list(Set);
encode_value(#woody_zset{content=ZSet}) ->
    encode_value(#woody_zset_result{content=zset:to_list(ZSet), scores=false});
encode_value(#woody_zset_result{content=ZSetResult, scores=Scores}) ->
    Fun = case Scores of
              true ->
                  fun({K, S, V}) ->
                          {K, {encode_value(V), 'WITHSCORE', S}}
                  end;
              false ->

                  fun({K, _S, V}) ->
                          {K, encode_value(V)}
                  end
          end,
    maps:from_list(lists:map(Fun, ZSetResult));

encode_value(#woody_trie{content=Trie}) ->
    Fun = fun(P, V, A) ->
                  maps:put(list_to_binary(P), V, A)
          end,
    trie:fold(Fun, #{}, Trie);

encode_value(V) ->
    V.

try_process_query(Key, Query, Value, Result) ->
    case process_query(Query, Value) of
        {error, _}=Error ->
            Error;
        undefined ->
            Result;
        V ->
            maps:put(Key, encode_value(V), Result)
    end.

process_query_key(Key, Query, undefined, Result) ->
    try_process_query(Key, Query, undefined, Result);

process_query_key(Key, Query, #woody_trie{content=Content}, Result) when is_binary(Key) ->
    Value = case trie:find(binary_to_list(Key), Content) of
                error ->
                    undefined;
                {ok, V} ->
                    V
            end,
    try_process_query(Key, Query, Value, Result);

process_query_key(Key, Query, #woody_zset{content=Content}, Result) ->
    Value = case zset:find(Key, Content) of
                error ->
                    undefined;
                {Key, _, V} ->
                    V
            end,
    try_process_query(Key, Query, Value, Result);

process_query_key(Key, Query, {dict, Content}, Result) ->
    Value = case dict:find(Key, Content) of
                error ->
                    undefined;
                {ok, V} ->
                    V
            end,
    try_process_query(Key, Query, Value, Result);

process_query_key(_Key, _Query, _, _Result) ->
    {error, unknown_query}.

process_query(Query, Content) when is_map(Query) ->
    Fun = fun(_Key, _Command, Error={error, _}) ->
                  Error;
             (Key, Command, Result) ->
                  process_query_key(Key, Command, Content, Result)
          end,
    R = maps:fold(Fun, #{}, Query),
    case R of
        {error, _}=Error->
            Error;
        R ->
            case maps:size(R) of
                0 ->
                    undefined;
                _ ->
                    R
            end
    end;

process_query('GET', Value) ->
    Value;

process_query({'T_SIMILAR', _Prefix}, undefined) ->
    undefined;
process_query({'T_SIMILAR', Prefix}, #woody_trie{content=Trie}) when is_binary(Prefix) ->
    Fun = fun(K, V, Acc) ->
                  maps:put(list_to_binary(K), V, Acc)
          end,
    Result = trie:fold_similar(binary_to_list(Prefix), Fun, #{}, Trie),
    case maps:size(Result) of
        0 ->
            undefined;
        _ ->
            #woody_trie_result{content=Result}
    end;

process_query({'S_INTERSECTION', L}, undefined) when is_list(L) ->
    undefined;
process_query({'S_INTERSECTION', L}, #woody_set{content=Set}) when is_list(L) ->
    #woody_set{content=sets:intersection(Set, sets:from_list(L))};

process_query('Z_GET', undefined) ->
    undefined;
process_query('Z_GET', #woody_zset{content=ZSet}) ->
    #woody_zset_result{content=zset:to_list(ZSet), scores=true};

process_query({'Z_TOP', N}, undefined) when is_integer(N)->
    undefined;
process_query({'Z_TOP', N, 'WITHSCORES'}, undefined) when is_integer(N) ->
    undefined;
process_query({'Z_TOP', N}, #woody_zset{content=ZSet}) when is_integer(N) ->
    #woody_zset_result{content=zset:top(N, ZSet), scores=false};
process_query({'Z_TOP', N, 'WITHSCORES'}, #woody_zset{content=ZSet}) when is_integer(N) ->
    #woody_zset_result{content=zset:top(N, ZSet), scores=true};

process_query({'Z_TOP', N, 'FROMSCORE', Score}, undefined) when is_integer(N), is_integer(Score) ->
    undefined;
process_query({'Z_TOP', N, 'FROMSCORE', Score, 'WITHSCORES'}, undefined) when is_integer(N), is_integer(Score)->
    undefined;
process_query({'Z_TOP', N, 'FROMSCORE', Score}, #woody_zset{content=ZSet}) when is_integer(N), is_integer(Score) ->
    #woody_zset_result{content=zset:top(N, Score, ZSet), scores=false};
process_query({'Z_TOP', N, 'FROMSCORE', Score, 'WITHSCORES'}, #woody_zset{content=ZSet}) when is_integer(N), is_integer(Score)->
    #woody_zset_result{content=zset:top(N, Score, ZSet), scores=true};

process_query({'Z_RANGE', N, M}, undefined) when is_integer(N), is_integer(M) ->
    undefined;
process_query({'Z_RANGE', N, M, 'WITHSCORES'}, undefined) when is_integer(N), is_integer(M) ->
    undefined;
process_query({'Z_RANGE', N, M}, #woody_zset{content=ZSet}) when is_integer(N), is_integer(M) ->
    #woody_zset_result{content=zset:range(N, M, ZSet), scores=false};
process_query({'Z_RANGE', N, M, 'WITHSCORES'}, #woody_zset{content=ZSet}) when is_integer(N), is_integer(M) ->
    #woody_zset_result{content=zset:range(N, M, ZSet), scores=true};

process_query({'Z_PAGE', N, 'FROMKEY', _K}, undefined) when is_integer(N) ->
    undefined;
process_query({'Z_PAGE', N, 'FROMKEY', _K, 'WITHSCORES'}, undefined) when is_integer(N) ->
    undefined;
process_query({'Z_PAGE', N, 'FROMKEY', K}, #woody_zset{content=ZSet}) when is_integer(N) ->
    #woody_zset_result{content=zset:page(K, N, ZSet), scores=false};
process_query({'Z_PAGE', N, 'FROMKEY', K, 'WITHSCORES'}, #woody_zset{content=ZSet}) when is_integer(N) ->
    #woody_zset_result{content=zset:page(K, N, ZSet), scores=true};

process_query(_, _) ->
    {error, unknown_query}.

process_update_key(Prefix, Update={'T_PREFIX', _}, undefined) when is_binary(Prefix) ->
    process_update_key(Prefix, Update, #woody_trie{content=trie:new()});
process_update_key(Prefix, {'T_PREFIX', Update}, #woody_trie{content=Trie}) when is_binary(Prefix) ->
    StringPrefix = binary_to_list(Prefix),
    OldValue = case trie:find(StringPrefix, Trie) of
                   error ->
                       undefined;
                   {ok, OV} ->
                       OV
               end,
    case process_update(Update, OldValue) of
        {error, _}=Error ->
            Error;
        undefined ->
            Trie1 = trie:erase(StringPrefix, Trie),
            case trie:size(Trie1) of
                0 ->
                    undefined;
                _ ->
                    #woody_trie{content=Trie1}
            end;
        V ->
            #woody_trie{content=trie:store(StringPrefix, V, Trie)}
    end;

process_update_key(Key, Update={'Z_SETSCORE', Score, _}, undefined) when is_integer(Score) ->
    process_update_key(Key, Update, #woody_zset{content=zset:new()});

process_update_key(Key, {'Z_SETSCORE', Score, Update}, #woody_zset{content=ZSet}) when is_integer(Score) ->
    OldValue = case zset:find(Key, ZSet) of
                   {Key, _, OV} ->
                       OV;
                   error ->
                       undefined
               end,
    case process_update(Update, OldValue) of
        {error, _}=Error ->
            Error;
        undefined ->
            ZSet1 = zset:delete(Key, ZSet),
            case zset:size(ZSet1) of
                0 ->
                    undefined;
                _ ->
                    #woody_zset{content=ZSet1}
            end;
        V ->
            #woody_zset{content=zset:enter(Key, Score, V, ZSet)}
    end;

process_update_key(Key, Update, undefined) ->
    process_update_key(Key, Update, {dict, dict:new()});

process_update_key(Key, Update, {dict, Dict}) ->
    OldValue = case dict:find(Key, Dict) of
                   {ok, OV} ->
                       OV;
                   error ->
                       undefined
               end,
    case process_update(Update, OldValue) of
        {error, _}=Error ->
            Error;
        undefined ->
            Dict1 = dict:erase(Key, Dict),
            case dict:size(Dict1) of
                0 ->
                    undefined;
                _ ->
                    {dict, Dict1}
            end;
        V ->
            {dict, dict:store(Key, V, Dict)}
    end;

process_update_key(_, _, _) ->
    {error, invalid_update}.


process_update(Update, Content) when is_map(Update) ->
    Fun = fun(_Key, _Command, Error={error, _}) ->
                  Error;
             (Key, Command, Acc) ->
                  process_update_key(Key, Command, Acc)
          end,
    maps:fold(Fun, Content, Update);

process_update({'SET', Value}, _) ->
    Value;

process_update('UNSET', _) ->
    undefined;

process_update({'INC', Inc}, Value) when is_integer(Inc),
                                         is_integer(Value)->
    Value + Inc;

process_update({'INC', Inc}, undefined) when is_integer(Inc) ->
    Inc;

process_update({'S_UPDATE', Add, Remove}, #woody_set{content=Set}) when is_list(Add), is_list(Remove) ->
    #woody_set{content=sets:subtract(sets:union(Set, sets:from_list(Add)),
                                     sets:from_list(Remove))};

process_update({'S_UNION', Union}, #woody_set{content=Set}) when is_list(Union) ->
    #woody_set{content=sets:union(Set, sets:from_list(Union))};

process_update({'S_UNION', Union}, undefined) when is_list(Union) ->
    #woody_set{content=sets:from_list(Union)};

process_update({'S_ADD', Add}, #woody_set{content=Set}) ->
    #woody_set{content=sets:add_element(Add, Set)};

process_update({'S_ADD', Add}, undefined) ->
    #woody_set{content=sets:add_element(Add, sets:new())};

process_update({'S_REMOVE', Remove}, #woody_set{}=WoodySet) ->
    process_update({'S_SUBTRACT', [Remove]}, WoodySet);

process_update({'S_REMOVE', _Remove}, undefined) ->
    undefined;

process_update({'S_SUBTRACT', Subtract}, undefined) when is_list(Subtract) ->
    undefined;
process_update({'S_SUBTRACT', Subtract}, #woody_set{content=Set}) when is_list(Subtract) ->
    Set1 = case Subtract of
               [One] ->
                   sets:del_element(One, Set);
               _ ->
                   sets:subtract(Set, sets:from_list(Subtract))
           end,
    case sets:size(Set1) of
        0 ->
            undefined;
        _ ->
            #woody_set{content=Set1}
    end;

process_update({'WHERE', Predicate}, Value) ->
    case process_predicate(Predicate, Value) of
        true ->
            Value;
        false ->
            {error, predicate_failed}
    end;

process_update({'WHERE', Predicate, Operation}, Value) ->
    case process_predicate(Predicate, Value) of
        true ->
            process_update(Operation, Value);
        false ->
            {error, predicate_failed}
    end;

process_update(_, _) ->
    {error, unknown_update}.

process_predicate('UNDEF', V) ->
    V == undefined;

process_predicate({'EQ', V1}, V2) ->
    V2 == V1;

process_predicate({'NE', V1}, V2) ->
    V2 /= V1;

process_predicate({'GT', V1}, V2) ->
    V2 > V1;

process_predicate({'GTE', V1}, V2) ->
    V2 >= V1;

process_predicate({'LT', V1}, V2) ->
    V2 < V1;

process_predicate({'LTE', V1}, V2) ->
    V2 =< V1;

process_predicate({'NOT', P}, V) ->
    not process_predicate(P, V);

process_predicate({'AND', []}, _V) ->
    true;

process_predicate({'AND', [H | T]}, V) ->
    process_predicate(H, V) andalso process_predicate({'AND', T}, V);

process_predicate({'OR', []}, _V) ->
    false;

process_predicate({'OR', [H | T]}, V) ->
    process_predicate(H, V) orelse process_predicate({'OR', T}, V);

process_predicate({'S_SUBSET', L}, #woody_set{content=Set}) when is_list(L) ->
    sets:is_subset(sets:from_list(L), Set);

process_predicate({'S_IN', I}, #woody_set{content=Set}) ->
    sets:is_element(I, Set);

process_predicate({'S_LEN', P}, undefined) ->
    process_predicate(P, 0);

process_predicate({'S_LEN', P}, #woody_set{content=Set}) ->
    process_predicate(P, sets:size(Set));

process_predicate('S_EMPTY', undefined) ->
    true;

process_predicate('S_EMPTY', #woody_set{content=Set}) ->
    sets:size(Set) == 0;

process_predicate(_, _) ->
    false.
