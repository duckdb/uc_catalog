include_directories(include)

add_subdirectory(storage)

add_library(
  uc_ext_library OBJECT uc_catalog_extension.cpp uc_api.cpp uc_utils.cpp)

target_link_libraries(uc_ext_library CURL::libcurl)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:uc_ext_library>
    PARENT_SCOPE)
