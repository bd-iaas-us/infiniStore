#ifndef LIBCONFIG_H
#define LIBCONFIG_H

typedef struct ServerConfig {
    int service_port;
    int manage_port;
    std::string log_level;
    size_t prealloc_size;
} server_config_t;

typedef struct ClientConfig {
    int service_port;
    int manage_port;
    std::string log_level;
    std::string host_addr;
} client_config_t;

#endif 