-module(woody_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0]).
-export([test_set/1,
         test_unknown_update/1,
         test_unknown_query/1,
         test_invalid_update/1,
         test_unset/1,
         test_inc/1,
         test_sets/1,
         test_zsets/1,
         test_where/1,
         test_trie/1,
         test_complex/1,
         test_is_woody/1
        ]).

all() -> [test_set,
          test_unknown_update,
          test_unknown_query,
          test_invalid_update,
          test_unset,
          test_inc,
          test_sets,
          test_zsets,
          test_where,
          test_trie,
          test_complex,
          test_is_woody
         ].

test_set(_Config) ->
    Tree = woody:new(),
    Update = #{1 => {'SET', <<"1">>}},
    {ok, Tree1} = woody:update(Update, Tree),

    Query = #{1 => 'GET'},
    ExpectedData = #{1 => <<"1">>},
    {ok, RawData} = woody:query(Query, Tree1),
    ExpectedData = woody:encode(RawData),

    {ok, Tree2} = woody:update(Update, Tree1),

    Query2 = #{1 => 'GET', 2 => 'GET'},
    ExpectedData2 = #{1 => <<"1">>},
    {ok, RawData2} = woody:query(Query2, Tree2),
    ExpectedData2 = woody:encode(RawData2),
    ok.

test_unknown_update(_Config) ->
    Tree = woody:new(),
    Update1 = #{1 => <<"1">>, 2 => <<"2">>},
    {error, unknown_update} = woody:update(Update1, Tree),
    Update2 = #{1 => {'Z_SETSCORE', 10, 'OLOLO'}},
    {error, unknown_update} = woody:update(Update2, Tree),
    ok.

test_unknown_query(_Config) ->
    Tree = woody:new(),
    Query = #{1 => <<"1">>, 2 => <<"2">>},
    {error, unknown_query} = woody:query(Query, Tree),

    Update = {'SET', <<"1">>},
    {ok, Tree1} = woody:update(Update, Tree),
    {error, unknown_query} = woody:query(Query, Tree1),

    ok.

test_invalid_update(_Config) ->
    Tree = woody:new(),
    Update = {'SET', <<"1">>},
    {ok, Tree1} = woody:update(Update, Tree),
    Update1 = #{1 => {'Z_SETSCORE', 10, 'UNSET'}},
    {error, invalid_update} = woody:update(Update1, Tree1),
    ok.

test_unset(_Config) ->
    Tree = woody:new(),
    Update = #{1 => {'SET', <<"1">>}},
    {ok, Tree1} = woody:update(Update, Tree),
    Update1 = #{1 => 'UNSET'},
    {ok, Tree} = woody:update(Update1, Tree1),


    Update2 = #{1 => #{2 => {'SET', <<"2">>}}},
    {ok, Tree2} = woody:update(Update2, Tree),
    Update3 = #{1 => #{2 => 'UNSET'}},
    {ok, Tree} = woody:update(Update3, Tree2),
    ok.

test_inc(_Config) ->
    Tree = woody:new(),
    Update = #{1 => {'INC', 1}},
    {ok, Tree1} = woody:update(Update, Tree),
    {ok, Tree2} = woody:update(Update, Tree1),

    Query = #{1 => 'GET'},
    ExpectedData = #{1 => 2},
    {ok, RawData} = woody:query(Query, Tree2),
    ExpectedData = woody:encode(RawData),
    ok.

test_sets(_Config) ->
    Tree = woody:new(),
    Update = #{1 => {'S_ADD', <<"1">>}},
    {ok, Tree1} = woody:update(Update, Tree),
    {ok, Tree2} = woody:update(Update, Tree1),

    Query1 = 'GET',
    ExpectedData1 = #{1 => [<<"1">>]},
    {ok, RawData1} = woody:query(Query1, Tree2),
    ExpectedData1 = woody:encode(RawData1),

    Update2 = #{1 => {'S_UNION', [<<"1">>]}},
    {ok, Tree3} = woody:update(Update2, Tree),
    {ok, Tree4} = woody:update(Update2, Tree3),
    {ok, RawData1} = woody:query(Query1, Tree4),

    Query3 = #{1 => {'S_INTERSECTION', [<<"1">>, <<"2">>]}},
    ExpectedData3 = #{1 => [<<"1">>]},
    {ok, RawData3} = woody:query(Query3, Tree4),
    ExpectedData3 = woody:encode(RawData3),

    Update4 = #{1 => {'S_REMOVE', <<"1">>}},
    Update5 = #{1 => {'S_SUBTRACT', [<<"1">>, <<"2">>]}},
    Update6 = #{1 => {'S_SUBTRACT', [<<"2">>]}},
    {ok, Tree} = woody:update(Update4, Tree4),
    {ok, Tree} = woody:update(Update5, Tree4),
    {ok, Tree} = woody:update(Update5, Tree),
    {ok, Tree} = woody:update(Update4, Tree),
    {ok, Tree2} = woody:update(Update6, Tree4),

    Update7 = #{1 => {'S_UPDATE', [<<"1">>], [<<"2">>]}},
    {ok, Tree2} = woody:update(Update7, Tree),
    {ok, Tree2} = woody:update(Update7, Tree1),

    Update8 = #{1 => {'S_UPDATE', [], [<<"1">>]}},
    {ok, Tree} = woody:update(Update8, Tree1),

    {ok, RawUndefined} = woody:query(Query3, Tree),
    undefined = woody:encode(RawUndefined),

    Query4 = {'S_INTERSECTION', [<<"1">>, <<"2">>]},
    {ok, RawUndefined} = woody:query(Query4, Tree),

    ok.

test_zsets(_Config) ->
    Tree = woody:new(),
    Update1 = #{1 => {'Z_SETSCORE', -10, {'SET', <<"1">>}}},
    {ok, Tree1} = woody:update(Update1, Tree),
    {ok, Tree1} = woody:update(Update1, Tree1),
    Update2 = #{1 => {'Z_SETSCORE', -10}},
    {ok, Tree1} = woody:update(Update2, Tree1),

    Query1 = #{1 => 'GET', 2 => 'GET'},
    ExpectedData1 = #{1 => <<"1">>},
    {ok, RawData1} = woody:query(Query1, Tree1),
    ExpectedData1 = woody:encode(RawData1),

    Query2 = {'Z_TOP', 5, 'WITHSCORES'},
    ExpectedData2 = #{1 => {<<"1">>, 'WITHSCORE', -10}},
    {ok, RawData2} = woody:query(Query2, Tree1),
    ExpectedData2 = woody:encode(RawData2),
    {ok, RawUndefined} = woody:query(Query2, Tree),
    undefined = woody:encode(RawUndefined),

    Query3 = {'Z_TOP', 5, 'FROMSCORE', -10, 'WITHSCORES'},
    {ok, RawData3} = woody:query(Query3, Tree1),
    ExpectedData2 = woody:encode(RawData3),
    {ok, RawUndefined} = woody:query(Query3, Tree),

    Query4 = {'Z_RANGE', -10, -5, 'WITHSCORES'},
    {ok, RawData4} = woody:query(Query4, Tree1),
    ExpectedData2 = woody:encode(RawData4),
    {ok, RawUndefined} = woody:query(Query4, Tree),

    Query5 = {'Z_PAGE', 5, 'FROMKEY', 1, 'WITHSCORES'},
    {ok, RawData4} = woody:query(Query5, Tree1),
    ExpectedData2 = woody:encode(RawData4),
    {ok, RawUndefined} = woody:query(Query5, Tree),

    Query6 = 'GET',
    {ok, RawData6} = woody:query(Query6, Tree1),
    ExpectedData1 = woody:encode(RawData6),

    Update7 = #{1 => {'Z_SETSCORE', -10, 'UNSET'}},
    {ok, Tree} = woody:update(Update7, Tree),

    Query8 = {'Z_TOP', 5},
    {ok, RawData8} = woody:query(Query8, Tree1),
    ExpectedData1 = woody:encode(RawData8),
    {ok, RawUndefined} = woody:query(Query8, Tree),

    Query9 = {'Z_TOP', 5, 'FROMSCORE', -10},
    {ok, RawData9} = woody:query(Query9, Tree1),
    ExpectedData1 = woody:encode(RawData9),
    {ok, RawUndefined} = woody:query(Query9, Tree),

    Query10 = {'Z_RANGE', -10, -5},
    {ok, RawData10} = woody:query(Query10, Tree1),
    ExpectedData1 = woody:encode(RawData10),
    {ok, RawUndefined} = woody:query(Query10, Tree),

    Query11 = {'Z_PAGE', 5, 'FROMKEY', 1},
    {ok, RawData11} = woody:query(Query11, Tree1),
    ExpectedData1 = woody:encode(RawData11),
    {ok, RawUndefined} = woody:query(Query11, Tree),

    Update12 = #{2 => {'Z_SETSCORE', 10, 'UNSET'}},
    {ok, Tree1} = woody:update(Update12, Tree1),

    Query13 =  'Z_GET',
    {ok, RawData13} = woody:query(Query13, Tree1),
    ExpectedData2 = woody:encode(RawData13),
    {ok, RawUndefined} = woody:query(Query13, Tree),
    ok.

test_where(_Config) ->
    Tree = woody:new(),
    Update = #{1 => {'SET', 10}},
    {ok, Tree1} = woody:update(Update, Tree),

    Update1 = #{1 => {'WHERE', {'EQ', 10}}},
    {ok, Tree1} = woody:update(Update1, Tree1),

    Update2 = #{1 => {'WHERE', {'NE', 10}}},
    {error, predicate_failed} = woody:update(Update2, Tree1),

    Update3 = #{1 => {'WHERE', {'EQ', 10}, {'SET', 20}}},
    {ok, Tree2} = woody:update(Update3, Tree1),
    Query3 = 'GET',
    ExpectedData3 = #{1 => 20},
    {ok, RawData3} = woody:query(Query3, Tree2),
    ExpectedData3 = woody:encode(RawData3),

    Update4 = #{1 => {'WHERE', {'NE', 10}, {'SET', 20}}},
    {error, predicate_failed} = woody:update(Update4, Tree1),

    Update5 = #{2 => {'WHERE', 'UNDEF'}},
    {ok, Tree1} = woody:update(Update5, Tree1),

    Update6 = #{1 => {'WHERE', {'GT', 5}}},
    {ok, Tree1} = woody:update(Update6, Tree1),

    Update7 = #{1 => {'WHERE', {'GTE', 10}}},
    {ok, Tree1} = woody:update(Update7, Tree1),

    Update8 = #{1 => {'WHERE', {'LT', 15}}},
    {ok, Tree1} = woody:update(Update8, Tree1),

    Update9 = #{1 => {'WHERE', {'LTE', 10}}},
    {ok, Tree1} = woody:update(Update9, Tree1),

    Update10 = #{1 => {'WHERE', {'AND',
                                 [{'GTE', 10},
                                  {'LTE', 10}]}}},
    {ok, Tree1} = woody:update(Update10, Tree1),

    Update11 = #{1 => {'WHERE', {'OR',
                                 [{'GT', 100},
                                  {'LT', 5}]}}},
    {error, predicate_failed} = woody:update(Update11, Tree1),

    Update12 = #{1 => {'WHERE', {'NOT', 'UNDEF'}}},
    {ok, Tree1} = woody:update(Update12, Tree1),

    Update13 = #{2 => {'WHERE', {'GT', 5}}},
    {error, predicate_failed} = woody:update(Update13, Tree1),

    Update14 = #{2 => {'WHERE', {'GTE', 10}}},
    {error, predicate_failed} = woody:update(Update14, Tree1),

    Update15 = #{2 => {'WHERE', {'LT', 15}}},
    {ok, Tree1} = woody:update(Update15, Tree1),

    Update16 = #{2 => {'WHERE', {'LTE', 10}}},
    {ok, Tree1} = woody:update(Update16, Tree1),

    SetsUpdate = #{1 => {'S_UNION', [1,2,3]}},
    {ok, SetsTree} = woody:update(SetsUpdate, Tree),

    SetsUpdate1 = #{1 => {'WHERE', {'S_IN', 1}}},
    {ok, SetsTree} = woody:update(SetsUpdate1, SetsTree),

    SetsUpdate2 = #{1 => {'WHERE', {'S_SUBSET', [1, 2]}}},
    {ok, SetsTree} = woody:update(SetsUpdate2, SetsTree),

    SetsUpdate3 = #{1 => {'WHERE', {'NOT', 'S_EMPTY'}}},
    {ok, SetsTree} = woody:update(SetsUpdate3, SetsTree),

    SetsUpdate4 = #{2 => {'WHERE', 'S_EMPTY'}},
    {ok, SetsTree} = woody:update(SetsUpdate4, SetsTree),

    SetsUpdate5 = #{1 => {'WHERE', 'UNKNOWN'}},
    {error, predicate_failed} = woody:update(SetsUpdate5, SetsTree),

    SetsUpdate6 = #{1 => {'WHERE', {'S_LEN', {'EQ', 3}}}},
    {ok, SetsTree} = woody:update(SetsUpdate6, SetsTree),

    SetsUpdate7 = #{2 => {'WHERE', {'S_LEN', {'EQ', 0}}}},
    {ok, SetsTree} = woody:update(SetsUpdate7, SetsTree),
    ok.

test_trie(_Config) ->
    Tree = woody:new(),
    GetQuery = 'GET',
    Update1 = #{<<"prefix">> => {'T_PREFIX', {'SET', <<"1">>}}},
    {ok, Tree1} = woody:update(Update1, Tree),
    {ok, Tree1} = woody:update(Update1, Tree1),
    Expected1 = #{<<"prefix">> => <<"1">>},
    {ok, RawData1} = woody:query(GetQuery, Tree1),
    Expected1 = woody:encode(RawData1),

    Update2 = #{<<"prefix">> => {'T_PREFIX', 'UNKNOWN'}},
    {error, unknown_update} = woody:update(Update2, Tree),

    Update3 = #{<<"prefix2">> => {'T_PREFIX', {'SET', <<"2">>}}},
    {ok, Tree3} = woody:update(Update3, Tree1),
    Expected3 = #{<<"prefix">> => <<"1">>,
                  <<"prefix2">> => <<"2">>},
    {ok, RawData3} = woody:query(GetQuery, Tree3),
    Expected3 = woody:encode(RawData3),

    Update4 = #{<<"prefix2">> => {'T_PREFIX', 'UNSET'}},
    {ok, Tree4} = woody:update(Update4, Tree3),
    {ok, RawData4} = woody:query(GetQuery, Tree4),
    Expected1 = woody:encode(RawData4),

    Update5 = #{<<"prefix">> => {'T_PREFIX', 'UNSET'}},
    {ok, Tree5} = woody:update(Update5, Tree4),
    {ok, RawData5} = woody:query(GetQuery, Tree5),
    undefined = woody:encode(RawData5),

    Query6 = #{<<"prefix">> => 'GET'},
    Expected6 = #{<<"prefix">> => <<"1">>},
    {ok, RawData6} = woody:query(Query6, Tree3),
    Expected6 = woody:encode(RawData6),

    Query7 = #{<<"unknown_prefix">> => 'GET'},
    {ok, RawData7} = woody:query(Query7, Tree3),
    undefined = woody:encode(RawData7),

    Update8 = #{<<"prefix">> => {'T_PREFIX', #{1 => {'SET', <<"11">>},
                                           2 => {'SET', <<"21">>}}},
                <<"prefix2">> => {'T_PREFIX', #{1 => {'SET', <<"12">>},
                                            2 => {'SET', <<"22">>}}}},
    {ok, Tree8} = woody:update(Update8, Tree),

    Query9 = {'T_SIMILAR', <<"prefix">>},
    Expected9 = #{<<"prefix">> => #{1 => <<"11">>,
                                    2 => <<"21">>},
                  <<"prefix2">> => #{1 => <<"12">>,
                                     2 => <<"22">>}},
    {ok, RawData9} = woody:query(Query9, Tree8),
    Expected9 = woody:encode(RawData9),

    Query10 = {'T_SIMILAR', <<"prefix2">>},
    Expected10 = #{<<"prefix2">> => #{1 => <<"12">>, 2 => <<"22">>}},
    {ok, RawData10} = woody:query(Query10, Tree8),
    Expected10 = woody:encode(RawData10),

    Query11 = {'T_SIMILAR', <<"prefix2">>},
    {ok, RawData11} = woody:query(Query11, Tree),
    undefined = woody:encode(RawData11),

    Query12 = {'T_SIMILAR', <<"qwe">>},
    {ok, RawData12} = woody:query(Query12, Tree8),
    undefined = woody:encode(RawData12),

    ok.

test_complex(_Config) ->
    Tree = woody:new(),
    Update1 = #{<<"guild_search">> => #{<<"guild_name_1">> => {'T_PREFIX', {'WHERE', 'UNDEF', #{<<"guild_id_1">> => {'SET', <<"value">>}}}}}},
    {ok, Tree1} = woody:update(Update1, Tree),
    Query1 = 'GET',
    {ok, RawData1} = woody:query(Query1, Tree1),
    Expected1 = #{<<"guild_search">> => #{<<"guild_name_1">> => #{<<"guild_id_1">> => <<"value">>}}},
    Expected1 = woody:encode(RawData1),

    Update2 = #{<<"guild_search">> => #{<<"guild_name_1">> => {'T_PREFIX', {'WHERE', 'UNDEF', #{<<"guild_id_2">> => {'SET', <<"value">>}}}}}},
    {error, predicate_failed} = woody:update(Update2, Tree1),
    ok.


test_is_woody(_Config) ->
    Tree = woody:new(),
    true = woody:is_woody(Tree),
    false = woody:is_woody(1),
    ok.

