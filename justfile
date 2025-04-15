ci:
    nix build --file . check-all --log-format bar-with-logs
check-all:
    nom build --file . check-all
update-pins:
    npins update
update: update-pins
