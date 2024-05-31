cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME uc_catalog)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

add_subdirectory(src)

find_package(CURL REQUIRED)
include_directories(${CURL_INCLUDE_DIRS})
include_directories(CURL::libcurl)

build_static_extension(${TARGET_NAME} ${ALL_OBJECT_FILES})
build_loadable_extension(${TARGET_NAME} " " ${ALL_OBJECT_FILES})

target_link_libraries(${EXTENSION_NAME} CURL::libcurl)
target_link_libraries(${LOADABLE_EXTENSION_NAME} CURL::libcurl)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
