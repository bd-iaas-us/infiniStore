cmake_minimum_required(VERSION 3.18)
project(infinistore)

set(CMAKE_CXX_STANDARD 17)

add_subdirectory(src/client)
add_subdirectory(src/server)
add_subdirectory(src/common)
add_subdirectory(src/client_bindgen)
add_subdirectory(src/server_bindgen)

# target_link_libraries(_infinistore PRIVATE client_bindgen server_bindgen common server client)

# set_target_properties(_infinistore PROPERTIES
#     INSTALL_RPATH "$ORIGIN"
#     BUILD_WITH_INSTALL_RPATH TRUE
# )

install(TARGETS common server client client_bindgen server_bindgen DESTINATION ${SKBUILD_PROJECT_NAME})