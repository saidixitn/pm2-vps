module.exports = {
    apps: [
      {
        name: "daily-stats",
        script: "/home/dixit/daily_stats.py",
        interpreter: "/home/dixit/venv/bin/python",
        cwd: "/home/dixit/",              // so load_dotenv() finds .env
        autorestart: false,
        cron_restart: "0 9 * * *",             // 09:00 AM IST (server time)
        out_file: "/home/dixit/logs/daily_stats.out.log",
        error_file: "/home/dixit/logs/daily_stats.err.log",
        log_date_format: "YYYY-MM-DD HH:mm:ss"
      }
    ]
  };