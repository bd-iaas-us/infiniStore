#ifndef LIBCONFIG_H
#define LIBCONFIG_H

#include <string>

typedef struct ServerConfig {
    int service_port;
    std::string log_level;
    std::string dev_name;
    size_t prealloc_size;  // unit: GB
    int ib_port;
    std::string link_type;
} server_config_t;

typedef struct ClientConfig {
    int service_port;
    std::string log_level;
    std::string dev_name;
    std::string host_addr;
    int ib_port;
    std::string link_type;
} client_config_t;

#endif
