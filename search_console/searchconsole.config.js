module.exports = {
  apps : [{
    name: "searchconsole-bigquery",
    script: "searchconsole_bigquery.py",
    cwd: "/home/dixit/search_console",
    interpreter: "/home/dixit/venv/bin/python",
    cron_restart: "0 11 * * *",
    autorestart: false,
    error_file: "/home/dixit/logs/searchconsole_bigquery.err.log",
    out_file: "/home/dixit/logs/searchconsole_bigquery.out.log",
    time: true // Adds timestamps to your logs automatically
  }]
}
