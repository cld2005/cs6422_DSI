FROM --platform=linux/amd64 ubuntu:jammy as build
LABEL maintainer=kimnguyen@gatech.edu
ENV DEBIAN_FRONTEND=noninteractive
ENV INSTALL_DIR=/usr/local/

# update and install dependencies
CMD ["bash"]

RUN apt-get update && apt-get upgrade -y && apt-get install -y \
    apt-utils gcc g++ openssh-server gdb gdbserver rsync vim

RUN apt-get -y update && apt-get -y install build-essential \
       && apt-get -y install zip unzip git cmake make valgrind clang clang-tidy clang-format googletest zlib1g-dev libgflags-dev libbenchmark-dev libgtest-dev zsh curl git-all ninja-build autoconf automake libtool locales-all dos2unix tar

RUN sh -c "$(curl -fsSL https://raw.github.com/ohmyzsh/ohmyzsh/master/tools/install.sh)"

# add work directory
ADD . /dsi
WORKDIR /dsi

# Taken from - https://docs.docker.com/engine/examples/running_ssh_service/#environment-variables

RUN mkdir /var/run/sshd
RUN echo 'root:root' | chpasswd
RUN sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

# SSH login fix. Otherwise user is kicked off after login
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd

ENV NOTVISIBLE="in users profile"
RUN echo "export VISIBLE=now" >> /etc/profile

# 22 for ssh server. 7777 for gdb server.
EXPOSE 22 7777

RUN useradd -ms /bin/bash debugger
RUN echo 'debugger:pwd' | chpasswd

########################################################
# Add custom packages and development environment here
########################################################

########################################################

CMD ["/usr/sbin/sshd", "-D"]