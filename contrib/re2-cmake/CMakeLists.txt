# Copyright 2015 The RE2 Authors.  All Rights Reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

# This file was edited for ClickHouse

string(FIND ${CMAKE_CURRENT_BINARY_DIR} " " _have_space)
if(_have_space GREATER 0)
    message(FATAL_ERROR "Using spaces in build path [${CMAKE_CURRENT_BINARY_DIR}] highly not recommended. Library re2st will be disabled.")
endif()

set(SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/re2")

set(RE2_SOURCES
    ${SRC_DIR}/re2/bitmap256.cc
    ${SRC_DIR}/re2/bitstate.cc
    ${SRC_DIR}/re2/compile.cc
    ${SRC_DIR}/re2/dfa.cc
    ${SRC_DIR}/re2/filtered_re2.cc
    ${SRC_DIR}/re2/mimics_pcre.cc
    ${SRC_DIR}/re2/nfa.cc
    ${SRC_DIR}/re2/onepass.cc
    ${SRC_DIR}/re2/parse.cc
    ${SRC_DIR}/re2/perl_groups.cc
    ${SRC_DIR}/re2/prefilter.cc
    ${SRC_DIR}/re2/prefilter_tree.cc
    ${SRC_DIR}/re2/prog.cc
    ${SRC_DIR}/re2/re2.cc
    ${SRC_DIR}/re2/regexp.cc
    ${SRC_DIR}/re2/set.cc
    ${SRC_DIR}/re2/simplify.cc
    ${SRC_DIR}/re2/tostring.cc
    ${SRC_DIR}/re2/unicode_casefold.cc
    ${SRC_DIR}/re2/unicode_groups.cc
    ${SRC_DIR}/util/rune.cc
    ${SRC_DIR}/util/strutil.cc
)
add_library(re2 ${RE2_SOURCES})
target_include_directories(re2 PUBLIC "${SRC_DIR}")
target_link_libraries(re2 ch_contrib::abseil_str_format)

add_library(ch_contrib::re2 ALIAS re2)
