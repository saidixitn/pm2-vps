module.exports = {
    apps: [
      {
        name: "Proxy FAQ's",
        script: "/home/dixit/proxy_faq.py",
        interpreter: "/home/dixit/venv/bin/python",
        autorestart: false,
        out_file: "/home/dixit/logs/proxy_faq.out.log",
        error_file: "/home/dixit/logs/proxy_faq.err.log"
      },
    ]
  };
  