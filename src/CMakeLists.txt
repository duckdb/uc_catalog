include_directories(include)

add_subdirectory(storage)

add_library(
  uc_ext_library OBJECT uc_catalog_extension.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:uc_ext_library>
    PARENT_SCOPE)
