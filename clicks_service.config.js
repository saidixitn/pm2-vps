module.exports = {
    apps: [
      {
        name: "run-clicks-cron",
        script: "./run_clicks.sh",
        instances: 1,
        exec_interpreter: "bash",
        exec_mode: "fork",
        cron_restart: "0 10,15 * * *", // Runs at 10:00 and 15:00
        autorestart: false,
        watch: false,
        time: true, // Prepends timestamps to your logs
        out_file: "run-clicks-out.log",
        error_file: "run-clicks-error.log"
      }
    ]
  };