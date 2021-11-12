FROM gitpod/workspace-base

############
### .Net ###
############
# Install .NET SDK (Current channel)
# Source: https://docs.microsoft.com/dotnet/core/install/linux-scripted-manual#scripted-install
USER gitpod
ENV DOTNET_VERSION=5.0
RUN mkdir -p /home/gitpod/dotnet && curl -fsSL https://dot.net/v1/dotnet-install.sh | bash /dev/stdin --channel ${DOTNET_VERSION} --install-dir /home/gitpod/dotnet
# This is a workaround until this bug has been resolved: https://github.com/gitpod-io/gitpod/issues/5090
# We copy the .Net installation to /tmp/dotnet in our prebuild script
# Once the bug has been fixed we should change the envs and remove the prebuild workaround
#ENV DOTNET_ROOT=/home/gitpod/dotnet
#ENV PATH=$PATH:/home/gitpod/dotnet
RUN cp -R /home/gitpod/dotnet /tmp/dotnet
ENV DOTNET_ROOT=/tmp/dotnet
ENV PATH=$PATH:/tmp/dotnet

# Install global .Net tools
# Note: Fantomas alpha version is required for Ionide -> Fantomas integration to work properly: https://github.com/ionide/ionide-vscode-fsharp/issues/1606
RUN dotnet tool install -g fantomas-tool --version 4.6.0-alpha-007
ENV PATH=$PATH:/home/gitpod/.dotnet/tools

##############
### Docker ###
##############
USER root
# https://docs.docker.com/engine/install/ubuntu/
RUN curl -o /var/lib/apt/dazzle-marks/docker.gpg -fsSL https://download.docker.com/linux/ubuntu/gpg \
    && apt-key add /var/lib/apt/dazzle-marks/docker.gpg \
    && add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
    && install-packages docker-ce=5:19.03.15~3-0~ubuntu-focal docker-ce-cli=5:19.03.15~3-0~ubuntu-focal containerd.io

RUN curl -o /usr/bin/slirp4netns -fsSL https://github.com/rootless-containers/slirp4netns/releases/download/v1.1.11/slirp4netns-$(uname -m) \
    && chmod +x /usr/bin/slirp4netns

RUN curl -o /usr/local/bin/docker-compose -fsSL https://github.com/docker/compose/releases/download/1.29.2/docker-compose-Linux-x86_64 \
    && chmod +x /usr/local/bin/docker-compose

USER gitpod
