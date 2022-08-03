FROM deepnote/python:3.9

RUN wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | sudo apt-key add -
RUN sudo apt update
RUN sudo apt install -y software-properties-common
RUN sudo add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/
RUN sudo apt update
RUN sudo apt install -y adoptopenjdk-8-hotspot

RUN curl -Lo coursier https://git.io/coursier-cli
RUN chmod +x coursier
RUN ./coursier launch --fork almond:0.11.2 --scala 2.12 -v -M almond.ScalaKernel -- --install
RUN rm -f coursier

ENV DEFAULT_KERNEL_NAME "scala"