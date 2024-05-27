add_library(
  uc_ext_storage OBJECT
  uc_catalog.cpp
  uc_catalog_set.cpp
  uc_clear_cache.cpp
  uc_schema_entry.cpp
  uc_schema_set.cpp
  uc_table_entry.cpp
  uc_table_set.cpp
  uc_transaction.cpp
  uc_transaction_manager.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:uc_ext_storage>
    PARENT_SCOPE)
