{ latest ? import <nixpkgs> { } }:

let
  pkgs = import (fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/tarball/dfec583872cf7ae087a5ff8659e6480798f16ba9";
    sha256 = "sha256:02mmzm1cc06ma0m0bmdx2awn208vb54yf9mcda6k3g2znzjr7lny";
  }) { };
in pkgs.mkShellNoCC {
  packages = (with pkgs; [
    maven
    jdk8
  ]);
}
