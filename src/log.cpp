#include "log.h"

static auto console = spdlog::stdout_color_mt(APP_NAME);

void set_log_level(std::string level) {
    if (level == "info") {
        console->set_level(spdlog::level::info);
    } else if (level == "error") {
        console->set_level(spdlog::level::err);
    } else if (level == "debug") {
        console->set_level(spdlog::level::debug);
    } else if (level == "warning") {
        console->set_level(spdlog::level::warn);
    }
}

void log_msg(std::string level, std::string msg) {
    if (level == "info") {
        spdlog::get(APP_NAME)->info(msg);
    } else if (level == "debug") {
        spdlog::get(APP_NAME)->debug(msg);
    } else if (level == "error") {
        spdlog::get(APP_NAME)->error(msg);
    } else if (level == "warning") {
        spdlog::get(APP_NAME)->warn(msg);
    }
}
