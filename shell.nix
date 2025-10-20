{ localpkgs ? import <nixpkgs> { } }:

let
  jdkpkgs = import (fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/archive/e6f23dc08d3624daab7094b701aa3954923c6bbb.tar.gz";
    sha256 = "sha256:0m0xmk8sjb5gv2pq7s8w7qxf7qggqsd3rxzv3xrqkhfimy2x7bnx";
  }) { };
  flatcpkgs = import (fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/archive/34bfa9403e42eece93d1a3740e9d8a02fceafbca.tar.gz";
    sha256 = "sha256:09d9fbbw9yndmllzks4gzzgqp4ka1pc1nacnqyarydz2xcixg8na";
  }) { };
  protocpkgs = import (fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/archive/4ab8a3de296914f3b631121e9ce3884f1d34e1e5.tar.gz";
    sha256 = "sha256:095mc0mlag8m9n9zmln482a32nmbkr4aa319f2cswyfrln9j41cr";
  }) { };
in localpkgs.mkShellNoCC {
  packages = (with localpkgs; [
      difftastic
      git
    ]) ++ (with jdkpkgs; [
      jdk8
      python3
    ]) ++ (with flatcpkgs; [
      flatbuffers
    ]) ++ (with protocpkgs; [
      protobuf3_19 
    ]);
  }
