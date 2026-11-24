module.exports = {
  apps: [
    {
      name: "to-post-api",
      script: "/home/dixit/to_post_api.py",
      interpreter: "/home/dixit/venv/bin/python",

      // DAEMON MODE:
      // Keeps the process alive. If it crashes, PM2 restarts it.
      autorestart: true,
      cron_restart: null,

      env: {
        PYTHONUNBUFFERED: "1",
        TZ: "Asia/Kolkata"
      },

      out_file: "/home/dixit/logs/to_post_api.out.log",
      error_file: "/home/dixit/logs/to_post_api.err.log"
    }
  ]
};
