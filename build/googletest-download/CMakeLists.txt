cmake_minimum_required(VERSION 3.8)

project(googletest-download NONE)

include(ExternalProject)
ExternalProject_Add(googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG main
        SOURCE_DIR "/home/diaa/finalProject2019/My real project/Database-Systems/build/googletest-src"
        BINARY_DIR "/home/diaa/finalProject2019/My real project/Database-Systems/build/googletest-build"
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
        TEST_COMMAND ""
        )
