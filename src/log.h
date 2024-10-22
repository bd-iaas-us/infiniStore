#ifndef __LOG_H__
#define __LOG_H__

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>

// give me some color see see

#define APP_NAME "infini"

#define DEBUG(fmt, ...) \
        do { spdlog::get(APP_NAME)->debug(fmt, ##__VA_ARGS__); } while (0)
#define ERROR(fmt, ...) \
        do { spdlog::get(APP_NAME)->error("[{}:{}] " fmt, __FILE__, __LINE__, ##__VA_ARGS__); } while (0)
#define INFO(fmt, ...) \
        do { spdlog::get(APP_NAME)->info(fmt, ##__VA_ARGS__); } while (0)
#define WARN(fmt, ...) \
        do { spdlog::get(APP_NAME)->warn("[{}:{}] " fmt, __FILE__, __LINE__, ##__VA_ARGS__); } while (0)

#define _stringify(_x) #_x
#define stringify(_x) _stringify(_x)

void set_log_level(std::string level);
void log_msg(std::string level, std::string msg);

#endif // __LOG_H__
