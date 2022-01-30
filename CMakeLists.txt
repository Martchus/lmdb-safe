cmake_minimum_required(VERSION 3.3.0 FATAL_ERROR)

project(lmdb-safe)

# define meta data
set(META_PROJECT_NAME lmdb-safe)
set(META_PROJECT_TYPE library)
set(META_PROJECT_VARNAME LMDB_SAFE)
set(META_APP_NAME "LMDB-Safe")
set(META_APP_AUTHOR "Martchus")
set(META_APP_URL "https://github.com/${META_APP_AUTHOR}/${META_PROJECT_NAME}")
set(META_APP_DESCRIPTION "A safe modern & performant C++ wrapper of LMDB")
set(META_VERSION_MAJOR 0)
set(META_VERSION_MINOR 0)
set(META_VERSION_PATCH 1)
set(META_VERSION_EXACT_SONAME ON)

# add project files
set(HEADER_FILES
    lmdb-safe.hh
    lmdb-typed.hh
    lmdb-reflective.hh
    lmdb-boost-serialization.hh)
set(SRC_FILES
    lmdb-safe.cc
    lmdb-typed.cc)

set(DOC_FILES README.md)

# find c++utilities
set(CONFIGURATION_PACKAGE_SUFFIX
    ""
    CACHE STRING "sets the suffix for find_package() calls to packages configured via c++utilities")
set(PACKAGE_NAMESPACE
    ""
    CACHE STRING "sets the namespace (prefix) for find_package() calls to packages configured via c++utilities")
if (PACKAGE_NAMESPACE)
    set(PACKAGE_NAMESPACE_PREFIX "${PACKAGE_NAMESPACE}-")
endif ()
find_package(${PACKAGE_NAMESPACE_PREFIX}c++utilities${CONFIGURATION_PACKAGE_SUFFIX} 5.5.0 REQUIRED)
use_cpp_utilities(ONLY_HEADERS VISIBILITY PUBLIC)
use_cpp_utilities(LIBRARIES_VARIABLE "TEST_LIBRARIES")

# find lmdb
include(3rdParty)
use_pkg_config_module(PKG_CONFIG_MODULES "lmdb")

# find boost libraries (required by tests)
option(BOOST_STATIC_LINKAGE "${STATIC_LINKAGE}" "link statically against Boost (instead of dynamically)")
set(Boost_USE_MULTITHREADED ON)
if (BOOST_STATIC_LINKAGE)
    set(Boost_USE_STATIC_LIBS ON)
endif ()
set(BOOST_ARGS "REQUIRED;COMPONENTS;serialization;iostreams")
use_package(TARGET_NAME Boost::iostreams PACKAGE_NAME Boost PACKAGE_ARGS "${BOOST_ARGS}" LIBRARIES_VARIABLE "TEST_LIBRARIES")
use_package(TARGET_NAME Boost::serialization PACKAGE_NAME Boost PACKAGE_ARGS "${BOOST_ARGS}" LIBRARIES_VARIABLE "TEST_LIBRARIES")

# find catch2 (required by tests)
use_package(TARGET_NAME Catch2::Catch2 PACKAGE_NAME Catch2 LIBRARIES_VARIABLE "TEST_LIBRARIES")

# find threading library (required by examples)
use_package(TARGET_NAME Threads::Threads PACKAGE_NAME Threads LIBRARIES_VARIABLE "TEST_LIBRARIES")

# include modules to apply configuration
include(BasicConfig)
include(WindowsResources)
include(LibraryTarget)
include(Doxygen)
include(ConfigHeader)

# configure test targets
include(TestUtilities)
list(APPEND TEST_LIBRARIES ${META_TARGET_NAME})
set(TESTS basic typed)
foreach (TEST ${TESTS})
    configure_test_target(TEST_NAME "${TEST}_tests" SRC_FILES "tests/${TEST}.cc" LIBRARIES "${TEST_LIBRARIES}")
endforeach ()
set(EXAMPLES basic multi rel resize scale typed view)
foreach (EXAMPLE ${EXAMPLES})
    configure_test_target(TEST_NAME "${EXAMPLE}_example" SRC_FILES "examples/${EXAMPLE}.cc" LIBRARIES "${TEST_LIBRARIES}")
endforeach ()
