{pkgs, ...}:
pkgs.writeShellScriptBin "unix-proxy" ''
  exec ${pkgs.socat}/bin/socat STDIO UNIX-CONNECT:$0.socket
''