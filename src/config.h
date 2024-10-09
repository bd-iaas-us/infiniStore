#ifndef LIBCONFIG_H
#define LIBCONFIG_H

typedef struct ServerConfig {
    int data_port;
    int control_port;
    std::string log_level;
    size_t prealloc_size;
} server_config_t;

typedef struct ClientConfig {
    int data_port;
    int control_port;
    std::string log_level;
    std::string connect_host;
} client_config_t;

#endif 