module.exports = {
  apps: [
    {
      // The API server — always running
      name: "clicks-api",
      script: "clicks_report.py",
      interpreter: "/home/dixit/venv/bin/python",
      cwd: "/home/dixit/Clicks Report",
      instances: 1,
      autorestart: true,
      max_restarts: 10,
      env: { PORT: "5000" },
    },
    {
      // The trigger — pm2 relaunches it at 10:00, 10:15, 15:00 (and 15:15,
      // which the script skips). The script decides yesterday vs today
      // from the clock:
      //   10:00 -> yesterday · 10:15 -> today · 15:00 -> today
      name: "clicks-trigger",
      script: "./run_report.sh",
      args: "scheduled",
      interpreter: "bash",
      cwd: "/home/dixit/Clicks Report",
      instances: 1,
      autorestart: false,
      cron_restart: "0,15 10,15 * * *",
    },
  ],
};