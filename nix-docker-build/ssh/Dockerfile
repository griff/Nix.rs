FROM alpine
ARG ARCH=x86_64
COPY --chown=0:0 files /
RUN wget https://hydra.nixos.org/job/nix/master/buildStatic.$ARCH-linux/latest/download/2/nix \
 && mv nix /bin/nix \
 && chmod a+x /bin/nix \
 && ln -s /bin/nix /bin/nix-build \
 && ln -s /bin/nix /bin/nix-channel \
 && ln -s /bin/nix /bin/nix-collect-garbage \
 && ln -s /bin/nix /bin/nix-copy-closure \
 && ln -s /bin/nix /bin/nix-daemon \
 && ln -s /bin/nix /bin/nix-env \
 && ln -s /bin/nix /bin/nix-hash \
 && ln -s /bin/nix /bin/nix-instantiate \
 && ln -s /bin/nix /bin/nix-prefetch-url \
 && ln -s /bin/nix /bin/nix-shell \
 && ln -s /bin/nix /bin/nix-store \
 && mkdir -m 0755 /etc/nix \
 && echo 'experimental-features = nix-command' > /etc/nix/nix.conf

