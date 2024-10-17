#include "log.h"

spdlog::level::level_enum get_log_level() {
    const char* log_level_env = std::getenv("INF_LOGLEVEL");
    if (log_level_env) {
        std::string log_level_str(log_level_env);
        if (log_level_str == "debug") {
            return spdlog::level::debug;
        } else if (log_level_str == "info") {
            return spdlog::level::info;
        } else if (log_level_str == "warn") {
            return spdlog::level::warn;
        } else if (log_level_str == "error") {
            return spdlog::level::err;
        } else if (log_level_str == "critical") {
            return spdlog::level::critical;
        }
    }
    return spdlog::level::info;
}

static auto console = spdlog::stdout_color_mt(APP_NAME);

int set_log_level(std::string level) {
    if (level == "info") {
        spdlog::get(APP_NAME)->set_level(spdlog::level::info);
    } else if (level == "error") {
        spdlog::get(APP_NAME)->set_level(spdlog::level::err);
    } else if (level == "debug") {
        spdlog::get(APP_NAME)->set_level(spdlog::level::debug);
    } else {
        return -1;
    }
    return 0;
}

static bool log_level_set = []() {
    console->set_level(get_log_level());
    return true;
}();
