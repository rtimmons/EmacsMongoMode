add_executable(helper
    helper.cpp
)

# Define headers for this library. PUBLIC headers are used for
# compiling the library, and will be added to consumers' build
# paths.
target_include_directories(helper
    PUBLIC
        ${Boost_INCLUDE_DIR}
        ${LIBMONGOCXX_INCLUDE_DIRS}
)


# Depend on libraries that we defined in the top-level CMakeLists.txt file
target_link_libraries(helper
    PUBLIC
        ${Boost_LIBRARIES}
        ${LIBMONGOCXX_LIBRARIES}
)
