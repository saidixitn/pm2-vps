module.exports = {
  apps: [
    {
      name: "bigquery-hourly-sync",
      script: "/home/dixit/bigquery/hourly_views.py",
      interpreter: "/home/dixit/venv/bin/python",
      autorestart: false,
      cron_restart: "5 * * * *",
      out_file: "/home/dixit/logs/bigquery_hourly.out.log",
      error_file: "/home/dixit/logs/bigquery_hourly.err.log",
      time: true
    },
  ]
};
